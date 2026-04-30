package jobscheduler;

import jobscheduler.core.JobScheduler;
import jobscheduler.dag.DagJobScheduler;

import java.util.*;

/**
 * Driver program demonstrating all scheduler capabilities:
 *
 * 1. One-time delayed jobs
 * 2. Recurring (fixed-rate) jobs
 * 3. Priority-ordered jobs
 * 4. Job cancellation
 * 5. DAG-based dependency execution
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       Job Scheduler with Parallelism — Demo     ║");
        System.out.println("╚══════════════════════════════════════════════════╝\n");

        // ═══════════════════════════════════════════════════════════════
        // Section 1: Core Scheduler (one-time, recurring, priority, cancel)
        // ═══════════════════════════════════════════════════════════════

        JobScheduler scheduler = new JobScheduler(2, 4);

        // ── 1a. One-time delayed job ────────────────────────────────
        scheduler.schedule(() -> {
            System.out.printf("  → [One-Time] Executed on %s at %d%n",
                    Thread.currentThread().getName(), System.currentTimeMillis());
        }, 500);

        // ── 1b. Recurring job (fires 3+ times during the demo) ──────
        String recurringId = scheduler.scheduleRecurring(() -> {
            System.out.printf("  → [Recurring] Heartbeat on %s at %d%n",
                    Thread.currentThread().getName(), System.currentTimeMillis());
        }, 200, 800);

        // ── 1c. Priority jobs (same delay, different priority) ──────
        // With delay=1000ms and priorities 1,3,10, they should execute
        // in order: priority-1, priority-3, priority-10
        scheduler.scheduleWithPriority(() -> {
            System.out.printf("  → [Priority=10] LOW priority executed on %s%n",
                    Thread.currentThread().getName());
        }, 1000, 10);

        scheduler.scheduleWithPriority(() -> {
            System.out.printf("  → [Priority=1] HIGH priority executed on %s%n",
                    Thread.currentThread().getName());
        }, 1000, 1);

        scheduler.scheduleWithPriority(() -> {
            System.out.printf("  → [Priority=3] MEDIUM priority executed on %s%n",
                    Thread.currentThread().getName());
        }, 1000, 3);

        // ── 1d. Cancelled job ───────────────────────────────────────
        String cancelledId = scheduler.schedule(() -> {
            System.out.println("  → [SHOULD NOT APPEAR] This job was cancelled!");
        }, 1500);
        scheduler.cancel(cancelledId);

        // Let the scheduler run for 4 seconds to observe recurring ticks
        System.out.println("\n[Main] Waiting 4 seconds for jobs to execute...\n");
        Thread.sleep(4000);

        // Shutdown core scheduler
        scheduler.shutdown();

        // ═══════════════════════════════════════════════════════════════
        // Section 2: DAG Scheduler
        // ═══════════════════════════════════════════════════════════════
        //
        //   A ──┐
        //       ├──▶ C ──┐
        //   B ──┘        ├──▶ E
        //       D ───────┘
        //
        //   A, B, D: independent roots (run in parallel)
        //   C: depends on A, B
        //   E: depends on C, D

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       DAG Job Scheduler — Demo                  ║");
        System.out.println("╚══════════════════════════════════════════════════╝\n");

        Map<String, Runnable> nodes = new LinkedHashMap<>();
        nodes.put("A", () -> simulateWork("A", 300));
        nodes.put("B", () -> simulateWork("B", 200));
        nodes.put("C", () -> simulateWork("C", 400));
        nodes.put("D", () -> simulateWork("D", 150));
        nodes.put("E", () -> simulateWork("E", 100));

        Map<String, List<String>> deps = new HashMap<>();
        deps.put("C", List.of("A", "B"));
        deps.put("E", List.of("C", "D"));

        DagJobScheduler dagScheduler = new DagJobScheduler(3);
        dagScheduler.executeDag(nodes, deps);
        dagScheduler.shutdown();

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║       All demos completed successfully!         ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
    }

    /** Simulates a CPU/IO-bound task with a configurable duration. */
    private static void simulateWork(String name, long durationMs) {
        try {
            System.out.printf("    [DAG Task %s] Working for %dms...%n", name, durationMs);
            Thread.sleep(durationMs);
            System.out.printf("    [DAG Task %s] Done!%n", name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf("    [DAG Task %s] Interrupted!%n", name);
        }
    }
}
