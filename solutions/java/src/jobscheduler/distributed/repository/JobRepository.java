package jobscheduler.distributed.repository;

import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.model.JobStatus;

import java.util.List;
import java.util.Optional;

/**
 * Persistence abstraction for job entities.
 *
 * In production this would be backed by Postgres with row-level
 * locking (SELECT ... FOR UPDATE) for CAS operations. The in-memory
 * stub uses synchronized blocks to simulate the same semantics.
 *
 * The DB is the durable source of truth. Redis is an ephemeral
 * scheduling index — if Redis loses data, the WatchdogService
 * can rebuild the queue from DB records in PENDING state.
 */
public interface JobRepository {

    void save(Job job);

    /**
     * Atomic compare-and-set on job status.
     *
     * This is the distributed locking primitive that prevents two
     * nodes from claiming the same job. In Postgres, this maps to:
     *   UPDATE jobs SET status=?, runner_node_id=?
     *   WHERE id=? AND status=?
     *   RETURNING id
     *
     * @param jobId    target job
     * @param expected current status (precondition)
     * @param next     desired status
     * @param nodeId   node claiming the job (null to clear)
     * @return true if the transition succeeded
     */
    boolean compareAndSetStatus(String jobId, JobStatus expected, JobStatus next, String nodeId);

    void updateHeartbeat(String jobId, long timestampMs);

    /**
     * Find jobs that are stuck in RUNNING state with a stale heartbeat.
     * Used by WatchdogService to recover crashed/dead nodes' jobs.
     *
     * @param thresholdMs absolute epoch millis — jobs with
     *                    lastHeartbeatMs < thresholdMs are stale
     */
    List<Job> findRunningJobsWithHeartbeatBefore(long thresholdMs);

    Optional<Job> findById(String jobId);
}
