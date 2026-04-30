package jobscheduler.distributed.service;

import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.model.JobStatus;
import jobscheduler.distributed.queue.RedisJobQueue;
import jobscheduler.distributed.repository.JobRepository;

/**
 * Client-facing API for job scheduling operations.
 *
 * Encapsulates the dual-write pattern: every scheduling operation
 * writes to BOTH the DB (source of truth) AND Redis (scheduling index).
 *
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ DUAL-WRITE CONSISTENCY                                          │
 * │                                                                  │
 * │ We write to DB first, then Redis. If Redis write fails, the     │
 * │ job exists in DB as PENDING but not in the queue. The           │
 * │ WatchdogService (or a periodic DB scan) can detect these        │
 * │ orphans and re-enqueue them.                                     │
 * │                                                                  │
 * │ The reverse order (Redis first, DB second) risks the job being  │
 * │ picked up by a node before the DB record exists, causing a      │
 * │ "job not found" error on CAS claim.                             │
 * │                                                                  │
 * │ For strict consistency, use a transactional outbox pattern:     │
 * │ write to DB in a transaction, have a CDC pipeline push to Redis.│
 * └──────────────────────────────────────────────────────────────────┘
 */
public class JobService {

    private final JobRepository repository;
    private final RedisJobQueue redisQueue;

    public JobService(JobRepository repository, RedisJobQueue redisQueue) {
        this.repository = repository;
        this.redisQueue = redisQueue;
    }

    /**
     * Schedule a one-time job.
     *
     * @param taskClassName class name for task resolution
     * @param payload       JSON payload for the task
     * @param delayMs       delay from now in milliseconds
     * @param priority      lower = higher priority
     * @return the created Job entity
     */
    public Job schedule(String name, String taskClassName, String payload,
                        long delayMs, int priority) {
        long executeAtMs = System.currentTimeMillis() + delayMs;

        Job job = new Job(name, taskClassName, payload,
                executeAtMs, -1, priority, 0);

        // DB first, Redis second (see dual-write note above)
        repository.save(job);
        redisQueue.enqueue(job.getId(), executeAtMs);

        System.out.printf("[JobService] Scheduled one-time job '%s' (%s) in %dms%n",
                name, job.getId().substring(0, 8), delayMs);

        return job;
    }

    /**
     * Schedule a recurring job.
     */
    public Job scheduleRecurring(String name, String taskClassName, String payload,
                                 long delayMs, long periodMs, int priority) {
        long executeAtMs = System.currentTimeMillis() + delayMs;

        Job job = new Job(name, taskClassName, payload,
                executeAtMs, periodMs, priority, 0);

        repository.save(job);
        redisQueue.enqueue(job.getId(), executeAtMs);

        System.out.printf("[JobService] Scheduled recurring job '%s' (%s) in %dms, " +
                        "period=%dms%n",
                name, job.getId().substring(0, 8), delayMs, periodMs);

        return job;
    }

    /**
     * Schedule a job with retry support.
     */
    public Job scheduleWithRetry(String name, String taskClassName, String payload,
                                 long delayMs, int priority, int maxRetries) {
        long executeAtMs = System.currentTimeMillis() + delayMs;

        Job job = new Job(name, taskClassName, payload,
                executeAtMs, -1, priority, maxRetries);

        repository.save(job);
        redisQueue.enqueue(job.getId(), executeAtMs);

        System.out.printf("[JobService] Scheduled retriable job '%s' (%s) in %dms, " +
                        "maxRetries=%d%n",
                name, job.getId().substring(0, 8), delayMs, maxRetries);

        return job;
    }

    /**
     * Cancel a pending job.
     *
     * Cancellation is best-effort: if the job is already RUNNING,
     * the CAS will fail and the job continues to completion.
     * Only PENDING jobs can be cancelled.
     */
    public boolean cancel(String jobId) {
        boolean cancelled = repository.compareAndSetStatus(
                jobId, JobStatus.PENDING, JobStatus.CANCELLED, null);

        if (cancelled) {
            redisQueue.remove(jobId);
            System.out.printf("[JobService] Cancelled job %s%n", jobId.substring(0, 8));
        } else {
            System.out.printf("[JobService] Could not cancel job %s (not in PENDING state)%n",
                    jobId.substring(0, 8));
        }

        return cancelled;
    }
}
