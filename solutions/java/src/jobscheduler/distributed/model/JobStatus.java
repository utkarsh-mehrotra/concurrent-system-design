package jobscheduler.distributed.model;

/**
 * Lifecycle states for a distributed job.
 *
 * State machine:
 *   PENDING ──▶ RUNNING ──▶ DONE
 *                  │
 *                  ├──▶ FAILED   (after maxRetries exhausted)
 *                  └──▶ PENDING  (watchdog recovery on stale heartbeat)
 *
 *   PENDING ──▶ CANCELLED  (client cancellation before execution)
 *
 * All transitions are guarded by compareAndSetStatus (CAS) in the
 * repository layer. This prevents two nodes from racing on the
 * same job — only the winner's CAS succeeds.
 */
public enum JobStatus {
    PENDING,
    RUNNING,
    DONE,
    FAILED,
    CANCELLED
}
