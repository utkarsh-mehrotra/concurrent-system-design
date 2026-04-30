package jobscheduler.distributed.watchdog;

import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.model.JobStatus;
import jobscheduler.distributed.queue.RedisJobQueue;
import jobscheduler.distributed.repository.JobRepository;

import java.util.List;
import java.util.concurrent.*;

/**
 * Watchdog service that detects and recovers orphaned jobs.
 *
 * When a scheduler node crashes (OOM, hardware failure, network
 * partition), its jobs remain in RUNNING state indefinitely with
 * no heartbeat updates. The watchdog periodically scans for such
 * stale jobs and resets them to PENDING so they get re-executed.
 *
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ STALE THRESHOLD CALIBRATION                                     │
 * │                                                                  │
 * │ Heartbeat interval: 5s                                           │
 * │ Stale threshold:    30s                                          │
 * │ Watchdog scan:      15s                                          │
 * │                                                                  │
 * │ A live node emits heartbeats every 5s. A 30s stale threshold    │
 * │ allows 6 missed heartbeats before declaring a job orphaned.     │
 * │ This tolerance window prevents false positives from GC pauses   │
 * │ or transient network blips.                                      │
 * │                                                                  │
 * │ Worst-case recovery latency: 30s (stale) + 15s (scan interval)  │
 * │ = ~45 seconds from node death to job re-execution.              │
 * └──────────────────────────────────────────────────────────────────┘
 */
public class WatchdogService implements Runnable {

    private final JobRepository repository;
    private final RedisJobQueue redisQueue;
    private final ScheduledExecutorService executor;

    private static final long STALE_THRESHOLD_MS = 30_000;     // 30 seconds
    private static final long SCAN_INTERVAL_MS   = 15_000;     // 15 seconds

    public WatchdogService(JobRepository repository, RedisJobQueue redisQueue) {
        this.repository = repository;
        this.redisQueue = redisQueue;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("watchdog");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        System.out.println("[Watchdog] Started — scanning every " + SCAN_INTERVAL_MS + "ms");
        executor.scheduleAtFixedRate(this, SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Scan for stale RUNNING jobs and reset them to PENDING.
     *
     * CAS is used to transition RUNNING → PENDING so that if the
     * original node is actually still alive (false positive),
     * only one of [watchdog, original node] will succeed in
     * transitioning the job — preventing a split-brain scenario.
     */
    @Override
    public void run() {
        try {
            long threshold = System.currentTimeMillis() - STALE_THRESHOLD_MS;
            List<Job> staleJobs = repository.findRunningJobsWithHeartbeatBefore(threshold);

            if (staleJobs.isEmpty()) return;

            System.out.printf("[Watchdog] Found %d stale job(s) — recovering%n", staleJobs.size());

            for (Job job : staleJobs) {
                // CAS: RUNNING → PENDING — clear the runner node
                boolean recovered = repository.compareAndSetStatus(
                        job.getId(), JobStatus.RUNNING, JobStatus.PENDING, null);

                if (recovered) {
                    // Re-enqueue in Redis at current time for immediate pickup
                    redisQueue.enqueue(job.getId(), System.currentTimeMillis());
                    System.out.printf("[Watchdog] ↻ Recovered job '%s' (%s) — " +
                                    "last heartbeat was %dms ago%n",
                            job.getName(), job.getId().substring(0, 8),
                            System.currentTimeMillis() - job.getLastHeartbeatMs());
                }
            }
        } catch (Exception e) {
            // Swallow to prevent ScheduledExecutorService from cancelling the task
            System.err.printf("[Watchdog] Scan error: %s%n", e.getMessage());
        }
    }

    public void shutdown() {
        executor.shutdown();
        System.out.println("[Watchdog] Shutdown complete");
    }
}
