package jobscheduler.distributed.node;

import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.model.JobStatus;
import jobscheduler.distributed.queue.RedisJobQueue;
import jobscheduler.distributed.repository.JobRepository;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * A single scheduler node in a distributed cluster.
 *
 * Each node independently polls Redis for due jobs, claims them
 * via CAS, executes tasks, and sends heartbeats. Multiple nodes
 * can run concurrently — CAS prevents double-execution.
 *
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ DISTRIBUTED COORDINATION MODEL                                   │
 * │                                                                  │
 * │  Node-1 ──┐                                                      │
 * │  Node-2 ──┼── poll Redis ZSET ──▶ CAS claim in DB ──▶ execute   │
 * │  Node-3 ──┘                                                      │
 * │                                                                  │
 * │ Only one node wins the CAS for each job. Losers skip silently.  │
 * ├──────────────────────────────────────────────────────────────────┤
 * │ AT-LEAST-ONCE DELIVERY GUARANTEE                                │
 * │                                                                  │
 * │ This scheduler provides at-least-once execution, NOT exactly-   │
 * │ once. If a node crashes after executing the task but before     │
 * │ marking it DONE, the WatchdogService will re-enqueue the job   │
 * │ and it will run again.                                           │
 * │                                                                  │
 * │ Exactly-once requires either:                                    │
 * │ (a) Idempotent tasks (safe to re-run without side effects), or  │
 * │ (b) Two-phase commit with the downstream system, or             │
 * │ (c) Transactional outbox pattern to dedup completed executions. │
 * │                                                                  │
 * │ For most job scheduling use cases, (a) is the pragmatic choice. │
 * └──────────────────────────────────────────────────────────────────┘
 */
public class DistributedSchedulerNode {

    private final String nodeId;
    private final JobRepository repository;
    private final RedisJobQueue redisQueue;
    private final ExecutorService jobExecutor;
    private final ScheduledExecutorService pollerExecutor;
    private final ScheduledExecutorService heartbeatExecutor;

    // Track actively running job IDs for heartbeat emission
    private final Set<String> activeJobIds = ConcurrentHashMap.newKeySet();

    private static final int POLL_INTERVAL_MS = 500;
    private static final int POLL_BATCH_SIZE = 10;
    private static final int HEARTBEAT_INTERVAL_MS = 5000;

    public DistributedSchedulerNode(JobRepository repository,
                                    RedisJobQueue redisQueue,
                                    int jobThreads) {
        this.nodeId = UUID.randomUUID().toString();
        this.repository = repository;
        this.redisQueue = redisQueue;

        this.jobExecutor = Executors.newFixedThreadPool(jobThreads, r -> {
            Thread t = new Thread(r);
            t.setName("node-" + nodeId.substring(0, 8) + "-exec-" + t.threadId());
            t.setDaemon(true);
            return t;
        });

        this.pollerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("node-" + nodeId.substring(0, 8) + "-poller");
            t.setDaemon(true);
            return t;
        });

        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("node-" + nodeId.substring(0, 8) + "-heartbeat");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start this node: begin polling Redis and emitting heartbeats.
     */
    public void start() {
        System.out.printf("[Node %s] Starting — polling every %dms, heartbeat every %dms%n",
                nodeId.substring(0, 8), POLL_INTERVAL_MS, HEARTBEAT_INTERVAL_MS);

        pollerExecutor.scheduleAtFixedRate(
                this::pollAndExecute,
                0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS
        );

        heartbeatExecutor.scheduleAtFixedRate(
                this::emitHeartbeats,
                HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS
        );
    }

    /**
     * Poll Redis for due jobs and attempt to claim + execute each one.
     */
    private void pollAndExecute() {
        try {
            List<String> dueJobIds = redisQueue.pollDueJobs(
                    System.currentTimeMillis(), POLL_BATCH_SIZE);

            for (String jobId : dueJobIds) {
                repository.findById(jobId).ifPresent(this::tryClaimAndExecute);
            }
        } catch (Exception e) {
            // Swallow exceptions in the poller to prevent ScheduledExecutorService
            // from silently cancelling the recurring task on unchecked exception.
            System.err.printf("[Node %s] Poll error: %s%n",
                    nodeId.substring(0, 8), e.getMessage());
        }
    }

    /**
     * Attempt to claim a job via CAS and execute it.
     *
     * CAS semantics: only ONE node across the cluster will succeed
     * in transitioning PENDING → RUNNING. All other nodes see the
     * CAS fail and skip the job. This is our distributed mutex.
     */
    private void tryClaimAndExecute(Job job) {
        // CAS: PENDING → RUNNING — this is the distributed lock acquisition
        boolean claimed = repository.compareAndSetStatus(
                job.getId(), JobStatus.PENDING, JobStatus.RUNNING, nodeId);

        if (!claimed) {
            // Another node won the race — expected in multi-node setup
            System.out.printf("[Node %s] CAS failed for job %s (another node claimed it)%n",
                    nodeId.substring(0, 8), job.getId().substring(0, 8));
            return;
        }

        System.out.printf("[Node %s] ✓ Claimed job '%s' (%s)%n",
                nodeId.substring(0, 8), job.getName(), job.getId().substring(0, 8));

        activeJobIds.add(job.getId());

        jobExecutor.submit(() -> executeJob(job));
    }

    /**
     * Execute the job task, handling success, failure, and retry logic.
     */
    private void executeJob(Job job) {
        String threadName = Thread.currentThread().getName();
        try {
            System.out.printf("[%s] ▶ Executing job '%s' (attempt %d/%d)%n",
                    threadName, job.getName(), job.getRetryCount() + 1, job.getMaxRetries() + 1);

            // Resolve and run the task by class name
            Runnable task = resolveTask(job.getTaskClassName(), job.getPayload());
            task.run();

            // ── Success path ────────────────────────────────────────
            activeJobIds.remove(job.getId());
            repository.compareAndSetStatus(
                    job.getId(), JobStatus.RUNNING, JobStatus.DONE, nodeId);

            System.out.printf("[%s] ✓ Job '%s' completed successfully%n",
                    threadName, job.getName());

            // Re-enqueue next occurrence for recurring jobs
            if (job.isRecurring()) {
                Job next = job.nextOccurrence();
                repository.save(next);
                redisQueue.enqueue(next.getId(), next.getExecuteAtMs());
                System.out.printf("[%s] ↻ Re-enqueued recurring job '%s' → %s (in %dms)%n",
                        threadName, next.getName(), next.getId().substring(0, 8), next.getPeriodMs());
            }

        } catch (Exception e) {
            // ── Failure path ────────────────────────────────────────
            activeJobIds.remove(job.getId());
            int currentRetry = job.getRetryCount();

            if (currentRetry < job.getMaxRetries()) {
                // Exponential backoff: 2^retryCount * 1000ms
                long backoffMs = (1L << currentRetry) * 1000L;
                long retryAtMs = System.currentTimeMillis() + backoffMs;

                // Reset to PENDING so another poll cycle picks it up
                job.setRetryCount(currentRetry + 1);
                repository.save(job);
                repository.compareAndSetStatus(
                        job.getId(), JobStatus.RUNNING, JobStatus.PENDING, null);
                redisQueue.enqueue(job.getId(), retryAtMs);

                System.out.printf("[%s] ✗ Job '%s' failed (attempt %d/%d), retrying in %dms: %s%n",
                        threadName, job.getName(), currentRetry + 1,
                        job.getMaxRetries() + 1, backoffMs, e.getMessage());
            } else {
                // Exhausted retries — mark as FAILED permanently
                repository.compareAndSetStatus(
                        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, nodeId);

                System.out.printf("[%s] ✗ Job '%s' FAILED permanently after %d attempts: %s%n",
                        threadName, job.getName(), currentRetry + 1, e.getMessage());
            }
        }
    }

    /**
     * Emit heartbeats for all actively running jobs.
     *
     * The WatchdogService uses stale heartbeats to detect crashed nodes.
     * If a node dies, its heartbeats stop, and after 30s the watchdog
     * reclaims the orphaned jobs.
     */
    private void emitHeartbeats() {
        long now = System.currentTimeMillis();
        for (String jobId : activeJobIds) {
            repository.updateHeartbeat(jobId, now);
        }
        if (!activeJobIds.isEmpty()) {
            System.out.printf("[Node %s] ♥ Heartbeat sent for %d active job(s)%n",
                    nodeId.substring(0, 8), activeJobIds.size());
        }
    }

    /**
     * Resolve a task Runnable from its class name + payload.
     *
     * In production, this would use a task registry or reflection
     * to instantiate the appropriate Runnable/Callable. Here we
     * use a simple switch for demo purposes.
     */
    private Runnable resolveTask(String taskClassName, String payload) {
        return switch (taskClassName) {
            case "PrintTask" -> () ->
                    System.out.printf("    [PrintTask] payload=%s on %s%n",
                            payload, Thread.currentThread().getName());

            case "SlowTask" -> () -> {
                try {
                    System.out.printf("    [SlowTask] Working (%s)...%n", payload);
                    Thread.sleep(2000);
                    System.out.printf("    [SlowTask] Done (%s)%n", payload);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };

            case "FailingTask" -> () -> {
                System.out.printf("    [FailingTask] About to fail (%s)%n", payload);
                throw new RuntimeException("Simulated failure: " + payload);
            };

            case "HeartbeatTask" -> () ->
                    System.out.printf("    [HeartbeatTask] tick (%s) at %d%n",
                            payload, System.currentTimeMillis());

            default -> () ->
                    System.out.printf("    [UnknownTask] class=%s, payload=%s%n",
                            taskClassName, payload);
        };
    }

    /**
     * Graceful shutdown:
     * 1. Stop heartbeats first (no new heartbeats for dead node)
     * 2. Stop poller (no new claims)
     * 3. Drain job executor (let in-flight tasks complete)
     */
    public void shutdown() {
        System.out.printf("[Node %s] Shutting down...%n", nodeId.substring(0, 8));

        heartbeatExecutor.shutdown();
        pollerExecutor.shutdown();
        jobExecutor.shutdown();

        try {
            if (!jobExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                List<Runnable> dropped = jobExecutor.shutdownNow();
                System.out.printf("[Node %s] Forced shutdown — %d task(s) dropped%n",
                        nodeId.substring(0, 8), dropped.size());
            }
        } catch (InterruptedException e) {
            jobExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.printf("[Node %s] Shutdown complete%n", nodeId.substring(0, 8));
    }

    public String getNodeId() { return nodeId; }
}
