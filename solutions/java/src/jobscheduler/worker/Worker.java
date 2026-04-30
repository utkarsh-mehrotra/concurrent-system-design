package jobscheduler.worker;

import jobscheduler.model.ScheduledJob;

import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;

/**
 * Dispatcher thread that continuously drains the DelayQueue and
 * hands mature jobs off to the job executor pool.
 *
 * Concurrency notes:
 * ─ queue.take() is a blocking call. The thread parks inside the
 *   DelayQueue's condition variable until a job matures, so there
 *   is zero busy-waiting and zero CPU spin.
 * ─ InterruptedException from take() is the *expected* shutdown
 *   signal (triggered by workerExecutor.shutdownNow()). We break
 *   the loop cleanly and let the thread terminate.
 * ─ The cancelledIds set is a ConcurrentHashMap-backed Set, so
 *   contains() is lock-free and wait-free on reads.
 * ─ Recurring jobs are re-enqueued immediately after submission
 *   (not after completion). This preserves fixed-rate semantics
 *   and avoids coupling the scheduling cadence to task duration.
 */
public class Worker implements Runnable {

    private final DelayQueue<ScheduledJob> queue;
    private final ExecutorService jobExecutor;
    private final Set<String> cancelledIds;

    public Worker(DelayQueue<ScheduledJob> queue,
                  ExecutorService jobExecutor,
                  Set<String> cancelledIds) {
        this.queue = queue;
        this.jobExecutor = jobExecutor;
        this.cancelledIds = cancelledIds;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.printf("[%s] Worker started%n", threadName);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Blocks until a job's delay expires — no polling, no spin.
                ScheduledJob job = queue.take();

                // ── Cancellation check ──────────────────────────────
                // Performed *after* dequeue so cancelled jobs are
                // silently drained rather than accumulating in the queue.
                if (cancelledIds.contains(job.getId())) {
                    System.out.printf("[%s] Skipped cancelled job %s%n",
                            threadName, job.getId().substring(0, 8));
                    continue;
                }

                // ── Submit to executor ──────────────────────────────
                System.out.printf("[%s] Dispatching job %s (priority=%d)%n",
                        threadName, job.getId().substring(0, 8), job.getPriority());

                jobExecutor.submit(() -> {
                    try {
                        job.getTask().run();
                    } catch (Exception e) {
                        System.err.printf("[JobExecutor] Uncaught exception in job %s: %s%n",
                                job.getId().substring(0, 8), e.getMessage());
                    }
                });

                // ── Re-enqueue recurring jobs ───────────────────────
                // Done immediately after submit(), not inside the
                // task's completion callback. This decouples scheduling
                // frequency from execution latency.
                if (job.isRecurring()) {
                    ScheduledJob next = job.nextOccurrence();
                    queue.put(next);
                    System.out.printf("[%s] Re-enqueued recurring job %s (next in %dms)%n",
                            threadName, next.getId().substring(0, 8), next.getPeriodMs());
                }

            } catch (InterruptedException e) {
                // Expected path: workerExecutor.shutdownNow() interrupts
                // threads blocked on take(). Restore interrupt flag and exit.
                System.out.printf("[%s] Worker interrupted — shutting down%n", threadName);
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.printf("[%s] Worker terminated%n", Thread.currentThread().getName());
    }
}
