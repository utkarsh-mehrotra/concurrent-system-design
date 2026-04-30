package jobscheduler.core;

import jobscheduler.model.ScheduledJob;
import jobscheduler.worker.Worker;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Central orchestrator for scheduling and executing jobs.
 *
 * Architecture (two-tier executor model):
 *
 *   ┌───────────────────────────┐
 *   │      DelayQueue           │  ← time-ordered, priority-tiebroken
 *   └──────────┬────────────────┘
 *              │ take()
 *   ┌──────────▼────────────────┐
 *   │  workerExecutor (N)       │  ← dispatcher threads (block on queue)
 *   └──────────┬────────────────┘
 *              │ submit()
 *   ┌──────────▼────────────────┐
 *   │  jobExecutor (M)          │  ← actual task execution pool
 *   └───────────────────────────┘
 *
 * Why two pools?
 * ─ Worker threads must remain parked on take() to drain the queue
 *   as fast as jobs mature. If workers also executed tasks, a long-
 *   running task would starve the dispatcher loop.
 * ─ The jobExecutor is a classic fixed thread pool that bounds
 *   parallelism for CPU/IO-bound work.
 *
 * Thread-safety:
 * ─ DelayQueue is internally synchronized.
 * ─ cancelledIds uses ConcurrentHashMap.newKeySet() — lock-free reads.
 * ─ All public methods are safe to call from any thread.
 */
public class JobScheduler {

    private final DelayQueue<ScheduledJob> queue;
    private final ExecutorService workerExecutor;
    private final ExecutorService jobExecutor;
    private final Set<String> cancelledIds;

    /**
     * @param workerThreads number of dispatcher threads draining the queue
     * @param jobThreads    number of threads executing submitted tasks
     */
    public JobScheduler(int workerThreads, int jobThreads) {
        this.queue = new DelayQueue<>();
        this.cancelledIds = ConcurrentHashMap.newKeySet();
        this.jobExecutor = Executors.newFixedThreadPool(jobThreads, r -> {
            Thread t = new Thread(r);
            t.setName("job-exec-" + t.threadId());
            t.setDaemon(true);
            return t;
        });
        this.workerExecutor = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r);
            t.setName("dispatcher-" + t.threadId());
            t.setDaemon(false); // non-daemon so JVM waits for clean shutdown
            return t;
        });

        // Start dispatcher threads
        for (int i = 0; i < workerThreads; i++) {
            workerExecutor.submit(new Worker(queue, jobExecutor, cancelledIds));
        }
        System.out.printf("[Scheduler] Initialized with %d dispatchers, %d job threads%n",
                workerThreads, jobThreads);
    }

    // ─── Scheduling API ─────────────────────────────────────────────

    /**
     * Schedule a one-time job to execute after {@code delayMs} milliseconds.
     * Returns the job ID for cancellation.
     */
    public String schedule(Runnable task, long delayMs) {
        ScheduledJob job = new ScheduledJob(task, delayMs, -1, 5);
        queue.put(job);
        System.out.printf("[Scheduler] Scheduled one-time job %s (delay=%dms)%n",
                job.getId().substring(0, 8), delayMs);
        return job.getId();
    }

    /**
     * Schedule a recurring job starting after {@code initialDelayMs} and
     * repeating every {@code periodMs} milliseconds.
     */
    public String scheduleRecurring(Runnable task, long initialDelayMs, long periodMs) {
        ScheduledJob job = new ScheduledJob(task, initialDelayMs, periodMs, 5);
        queue.put(job);
        System.out.printf("[Scheduler] Scheduled recurring job %s (delay=%dms, period=%dms)%n",
                job.getId().substring(0, 8), initialDelayMs, periodMs);
        return job.getId();
    }

    /**
     * Schedule a one-time job with explicit priority.
     * Lower priority value = dispatched first when multiple jobs mature
     * at the same instant.
     */
    public String scheduleWithPriority(Runnable task, long delayMs, int priority) {
        ScheduledJob job = new ScheduledJob(task, delayMs, -1, priority);
        queue.put(job);
        System.out.printf("[Scheduler] Scheduled priority job %s (delay=%dms, priority=%d)%n",
                job.getId().substring(0, 8), delayMs, priority);
        return job.getId();
    }

    /**
     * Cancel a job by ID. If the job hasn't been dequeued yet, the
     * dispatcher will skip it upon take(). No-op if already executed.
     *
     * Note: we do NOT remove from the DelayQueue (O(n) scan + heap
     * rebuild). Instead, the cancelled set acts as a tombstone — the
     * job is silently discarded when it surfaces. This is the standard
     * lazy-cancellation pattern used by ScheduledThreadPoolExecutor.
     */
    public boolean cancel(String jobId) {
        boolean added = cancelledIds.add(jobId);
        if (added) {
            System.out.printf("[Scheduler] Cancelled job %s%n", jobId.substring(0, 8));
        }
        return added;
    }

    // ─── Lifecycle ──────────────────────────────────────────────────

    /**
     * Graceful shutdown sequence:
     *
     * 1. shutdownNow() on workerExecutor → interrupts threads blocked
     *    on queue.take(), preventing new jobs from being dispatched.
     * 2. shutdown() on jobExecutor → stops accepting new submissions
     *    but lets in-flight tasks finish.
     * 3. awaitTermination(10s) → waits for running tasks to complete.
     * 4. If timeout → shutdownNow() + log how many tasks were dropped.
     */
    public void shutdown() {
        System.out.println("\n[Scheduler] ══════ Initiating shutdown ══════");

        // Phase 1: kill dispatchers (interrupt take() calls)
        List<Runnable> unprocessedWorkers = workerExecutor.shutdownNow();
        System.out.printf("[Scheduler] Phase 1: Interrupted %d dispatcher(s), " +
                "%d queued workers dropped%n", 0, unprocessedWorkers.size());

        // Phase 2: gracefully drain job executor
        jobExecutor.shutdown();
        System.out.println("[Scheduler] Phase 2: Job executor no longer accepting tasks");

        try {
            // Phase 3: wait for in-flight tasks
            if (jobExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("[Scheduler] Phase 3: All tasks completed cleanly");
            } else {
                // Phase 4: force-kill stragglers
                List<Runnable> dropped = jobExecutor.shutdownNow();
                System.out.printf("[Scheduler] Phase 4: Timeout exceeded — " +
                        "force-killed executor, %d task(s) dropped%n", dropped.size());
            }
        } catch (InterruptedException e) {
            jobExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.out.println("[Scheduler] Shutdown interrupted — forced immediate exit");
        }

        int remainingInQueue = queue.size();
        if (remainingInQueue > 0) {
            System.out.printf("[Scheduler] %d job(s) remaining in DelayQueue (abandoned)%n",
                    remainingInQueue);
        }

        System.out.println("[Scheduler] ══════ Shutdown complete ══════\n");
    }

    // ─── Accessors (for testing / observability) ─────────────────────

    public int getQueueSize()       { return queue.size(); }
    public int getCancelledCount()  { return cancelledIds.size(); }
    public DelayQueue<ScheduledJob> getQueue() { return queue; }
    public ExecutorService getJobExecutor() { return jobExecutor; }
}
