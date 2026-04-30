package jobscheduler.distributed.repository;

import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.model.JobStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory stub implementing JobRepository.
 *
 * Simulates a Postgres-backed repository using ConcurrentHashMap.
 * The synchronized block in compareAndSetStatus simulates
 * SELECT ... FOR UPDATE row-level locking.
 *
 * In production, replace with JPA/JDBC + Postgres.
 */
public class InMemoryJobRepository implements JobRepository {

    private final ConcurrentHashMap<String, Job> store = new ConcurrentHashMap<>();

    @Override
    public void save(Job job) {
        store.put(job.getId(), job);
    }

    /**
     * Synchronized on the Job instance to simulate row-level locking.
     *
     * Why not ConcurrentHashMap.compute()? Because CAS needs to check
     * the current status AND conditionally update multiple fields
     * (status + runnerNodeId) atomically. A synchronized block on
     * the entity is cleaner and mirrors the DB row lock semantics.
     */
    @Override
    public boolean compareAndSetStatus(String jobId, JobStatus expected, JobStatus next, String nodeId) {
        Job job = store.get(jobId);
        if (job == null) return false;

        synchronized (job) {
            if (job.getStatus() != expected) {
                return false;
            }
            job.setStatus(next);
            job.setRunnerNodeId(nodeId);
            if (next == JobStatus.RUNNING) {
                job.setLastHeartbeatMs(System.currentTimeMillis());
            }
            return true;
        }
    }

    @Override
    public void updateHeartbeat(String jobId, long timestampMs) {
        Job job = store.get(jobId);
        if (job != null) {
            job.setLastHeartbeatMs(timestampMs);
        }
    }

    @Override
    public List<Job> findRunningJobsWithHeartbeatBefore(long thresholdMs) {
        List<Job> stale = new ArrayList<>();
        for (Job job : store.values()) {
            if (job.getStatus() == JobStatus.RUNNING &&
                    job.getLastHeartbeatMs() < thresholdMs) {
                stale.add(job);
            }
        }
        return stale;
    }

    @Override
    public Optional<Job> findById(String jobId) {
        return Optional.ofNullable(store.get(jobId));
    }
}
