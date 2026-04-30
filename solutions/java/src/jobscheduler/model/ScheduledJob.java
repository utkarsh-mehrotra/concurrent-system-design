package jobscheduler.model;

import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Represents a schedulable unit of work.
 *
 * Implements {@link Delayed} for DelayQueue ordering (time-based) and
 * {@link Comparable} for priority tiebreaking when two jobs become
 * eligible at the same instant.
 *
 * Thread-safety: instances are effectively immutable once constructed.
 * The only mutation path is {@link #nextOccurrence()}, which returns
 * a *new* ScheduledJob rather than mutating state — this avoids
 * the subtle bug where modifying a Delayed element already inside
 * a DelayQueue would corrupt the heap invariant.
 */
public class ScheduledJob implements Delayed, Comparable<Delayed> {

    private final String id;
    private final Runnable task;
    private final long executeAtMs;   // absolute epoch millis
    private final long periodMs;      // -1 → one-shot
    private final int priority;       // lower value = higher priority

    public ScheduledJob(Runnable task, long delayMs, long periodMs, int priority) {
        this.id = UUID.randomUUID().toString();
        this.task = task;
        this.executeAtMs = System.currentTimeMillis() + delayMs;
        this.periodMs = periodMs;
        this.priority = priority;
    }

    /** Internal constructor used by {@link #nextOccurrence()} to preserve identity. */
    private ScheduledJob(String id, Runnable task, long executeAtMs, long periodMs, int priority) {
        this.id = id;
        this.task = task;
        this.executeAtMs = executeAtMs;
        this.periodMs = periodMs;
        this.priority = priority;
    }

    // ─── Delayed contract ────────────────────────────────────────────

    @Override
    public long getDelay(TimeUnit unit) {
        long remainingMs = executeAtMs - System.currentTimeMillis();
        return unit.convert(remainingMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Primary ordering: earliest executeAtMs first (natural DelayQueue order).
     * Tiebreaker: lower priority value wins. This ensures that if two jobs
     * mature at the same wall-clock instant, the higher-priority job is
     * dequeued first.
     */
    @Override
    public int compareTo(Delayed other) {
        if (this == other) return 0;

        if (other instanceof ScheduledJob otherJob) {
            int timeCompare = Long.compare(this.executeAtMs, otherJob.executeAtMs);
            if (timeCompare != 0) return timeCompare;
            return Integer.compare(this.priority, otherJob.priority);
        }

        // Fallback for non-ScheduledJob Delayed (shouldn't happen in practice)
        return Long.compare(
                this.getDelay(TimeUnit.MILLISECONDS),
                other.getDelay(TimeUnit.MILLISECONDS)
        );
    }

    // ─── Domain logic ────────────────────────────────────────────────

    public boolean isRecurring() {
        return periodMs > 0;
    }

    /**
     * Returns a new ScheduledJob shifted forward by one period.
     * Preserves the original ID so cancellation semantics carry over.
     *
     * Uses fixed-rate scheduling (anchored to executeAtMs + period)
     * rather than fixed-delay (anchored to now + period). This prevents
     * drift accumulation when the executor is temporarily saturated.
     */
    public ScheduledJob nextOccurrence() {
        if (!isRecurring()) {
            throw new IllegalStateException("nextOccurrence() called on a one-shot job: " + id);
        }
        return new ScheduledJob(id, task, executeAtMs + periodMs, periodMs, priority);
    }

    // ─── Accessors ───────────────────────────────────────────────────

    public String getId()          { return id; }
    public Runnable getTask()      { return task; }
    public long getExecuteAtMs()   { return executeAtMs; }
    public long getPeriodMs()      { return periodMs; }
    public int getPriority()       { return priority; }

    @Override
    public String toString() {
        return String.format("ScheduledJob{id=%s, executeAt=%d, period=%d, priority=%d}",
                id.substring(0, 8), executeAtMs, periodMs, priority);
    }
}
