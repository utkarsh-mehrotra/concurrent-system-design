package jobscheduler.distributed.model;

import java.util.UUID;

/**
 * Persistent job entity representing a schedulable unit of work
 * in a distributed environment.
 *
 * Unlike the in-process ScheduledJob (which holds a Runnable directly),
 * this entity stores a taskClassName + JSON payload so it can be
 * serialized to a database and picked up by any node in the cluster.
 *
 * Fields are mutable because the repository layer performs CAS
 * updates on status, retryCount, heartbeat, and runnerNodeId.
 * Thread-safety is delegated to the repository's CAS semantics.
 */
public class Job {

    private final String id;
    private final String name;
    private final String taskClassName;
    private final String payload;         // JSON string — opaque to scheduler
    private final long executeAtMs;       // absolute epoch millis for first execution
    private final long periodMs;          // -1 → one-shot; >0 → recurring interval
    private final int priority;           // lower = higher priority
    private final int maxRetries;

    // Mutable state — updated by scheduler nodes via CAS
    private volatile JobStatus status;
    private volatile int retryCount;
    private volatile long lastHeartbeatMs;
    private volatile String runnerNodeId; // which node currently owns execution

    public Job(String name, String taskClassName, String payload,
               long executeAtMs, long periodMs, int priority, int maxRetries) {
        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.taskClassName = taskClassName;
        this.payload = payload;
        this.executeAtMs = executeAtMs;
        this.periodMs = periodMs;
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.status = JobStatus.PENDING;
        this.retryCount = 0;
        this.lastHeartbeatMs = 0;
        this.runnerNodeId = null;
    }

    // ─── Copy constructor for recurring re-creation ─────────────────
    // Creates a new Job ID for the next occurrence so the previous
    // completed/failed instance remains in the DB as an audit trail.
    public Job nextOccurrence() {
        if (periodMs <= 0) {
            throw new IllegalStateException("nextOccurrence() on non-recurring job: " + id);
        }
        return new Job(name, taskClassName, payload,
                executeAtMs + periodMs, periodMs, priority, maxRetries);
    }

    public boolean isRecurring() {
        return periodMs > 0;
    }

    // ─── Accessors ──────────────────────────────────────────────────

    public String getId()             { return id; }
    public String getName()           { return name; }
    public String getTaskClassName()  { return taskClassName; }
    public String getPayload()        { return payload; }
    public long getExecuteAtMs()      { return executeAtMs; }
    public long getPeriodMs()         { return periodMs; }
    public int getPriority()          { return priority; }
    public int getMaxRetries()        { return maxRetries; }

    public JobStatus getStatus()      { return status; }
    public int getRetryCount()        { return retryCount; }
    public long getLastHeartbeatMs()  { return lastHeartbeatMs; }
    public String getRunnerNodeId()   { return runnerNodeId; }

    public void setStatus(JobStatus status)           { this.status = status; }
    public void setRetryCount(int retryCount)         { this.retryCount = retryCount; }
    public void setLastHeartbeatMs(long ms)           { this.lastHeartbeatMs = ms; }
    public void setRunnerNodeId(String nodeId)        { this.runnerNodeId = nodeId; }

    @Override
    public String toString() {
        return String.format("Job{id=%s, name='%s', status=%s, runner=%s, retries=%d/%d}",
                id.substring(0, 8), name, status,
                runnerNodeId != null ? runnerNodeId.substring(0, 8) : "none",
                retryCount, maxRetries);
    }
}
