package jobscheduler.dag;

import java.util.*;
import java.util.concurrent.*;

/**
 * Dependency-aware (DAG) job executor using CompletableFuture composition.
 *
 * Topological ordering is implicit — CompletableFuture.allOf() naturally
 * enforces that a node runs only after all its dependencies complete.
 * This eliminates the need for an explicit Kahn's algorithm or DFS sort.
 *
 * Architecture:
 *
 *   A ──┐
 *       ├──▶ C ──┐
 *   B ──┘        ├──▶ E
 *       D ───────┘
 *
 *   • A, B, D have no deps → start immediately (fan-out)
 *   • C waits on allOf(A, B) → fan-in
 *   • E waits on allOf(C, D) → fan-in
 *
 * Thread-safety: each execution creates its own futures map, so
 * executeDag() is safe to call concurrently for independent DAGs.
 */
public class DagJobScheduler {

    private final ExecutorService pool;

    public DagJobScheduler(int threads) {
        this.pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setName("dag-worker-" + t.threadId());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Accepts an externally-provided executor (e.g., reuse the
     * JobScheduler's job executor for unified thread management).
     */
    public DagJobScheduler(ExecutorService pool) {
        this.pool = pool;
    }

    /**
     * Execute a DAG defined by:
     *
     * @param nodes map of nodeId → Runnable task
     * @param deps  map of nodeId → list of dependency nodeIds
     *              (absent key or empty list = no dependencies)
     *
     * Blocks the calling thread until the entire DAG completes.
     *
     * @throws IllegalArgumentException if a dependency references an unknown node
     */
    public void executeDag(Map<String, Runnable> nodes, Map<String, List<String>> deps) {
        System.out.println("\n[DAG] ══════ Starting DAG execution ══════");
        System.out.printf("[DAG] Nodes: %s%n", nodes.keySet());
        System.out.printf("[DAG] Dependencies: %s%n", deps);

        // Validate: every dependency must reference a known node
        for (Map.Entry<String, List<String>> entry : deps.entrySet()) {
            for (String dep : entry.getValue()) {
                if (!nodes.containsKey(dep)) {
                    throw new IllegalArgumentException(
                            String.format("Node '%s' depends on unknown node '%s'",
                                    entry.getKey(), dep));
                }
            }
        }

        // ── Build CompletableFuture graph ───────────────────────────
        // Each node gets exactly one future. We process nodes in an
        // order that guarantees all dependencies are already in the
        // map when we look them up (simple topological iteration).
        Map<String, CompletableFuture<Void>> futures = new HashMap<>();

        for (String nodeId : topologicalOrder(nodes.keySet(), deps)) {
            Runnable task = nodes.get(nodeId);
            List<String> nodeDeps = deps.getOrDefault(nodeId, List.of());

            if (nodeDeps.isEmpty()) {
                // Root node — start immediately on the thread pool
                futures.put(nodeId, CompletableFuture.runAsync(() -> {
                    System.out.printf("[DAG] ▶ Executing root node '%s' on %s%n",
                            nodeId, Thread.currentThread().getName());
                    task.run();
                    System.out.printf("[DAG] ✓ Completed '%s'%n", nodeId);
                }, pool));
            } else {
                // Dependent node — fan-in on all dependencies
                CompletableFuture<?>[] depFutures = nodeDeps.stream()
                        .map(futures::get)
                        .toArray(CompletableFuture<?>[]::new);

                futures.put(nodeId,
                        CompletableFuture.allOf(depFutures)
                                .thenRunAsync(() -> {
                                    System.out.printf("[DAG] ▶ Executing node '%s' " +
                                                    "(deps=%s) on %s%n",
                                            nodeId, nodeDeps,
                                            Thread.currentThread().getName());
                                    task.run();
                                    System.out.printf("[DAG] ✓ Completed '%s'%n", nodeId);
                                }, pool));
            }
        }

        // ── Block until entire DAG completes ────────────────────────
        CompletableFuture<Void> allDone = CompletableFuture.allOf(
                futures.values().toArray(CompletableFuture[]::new));

        try {
            allDone.join();
            System.out.println("[DAG] ══════ DAG execution complete ══════\n");
        } catch (CompletionException e) {
            System.err.printf("[DAG] DAG execution failed: %s%n",
                    e.getCause().getMessage());
            throw e;
        }
    }

    /**
     * Kahn's algorithm for topological ordering.
     *
     * Guarantees that when we process node X, all of X's dependencies
     * have already been processed and their futures exist in the map.
     *
     * @throws IllegalStateException if the graph contains a cycle
     */
    private List<String> topologicalOrder(Set<String> nodeIds,
                                          Map<String, List<String>> deps) {
        // Build in-degree map
        Map<String, Integer> inDegree = new HashMap<>();
        for (String id : nodeIds) {
            inDegree.put(id, 0);
        }
        for (Map.Entry<String, List<String>> entry : deps.entrySet()) {
            if (nodeIds.contains(entry.getKey())) {
                inDegree.put(entry.getKey(),
                        inDegree.getOrDefault(entry.getKey(), 0) + entry.getValue().size());
            }
        }

        // Seed queue with zero-degree nodes
        Queue<String> ready = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        // Build reverse adjacency: dep → list of dependents
        Map<String, List<String>> reverseDeps = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : deps.entrySet()) {
            for (String dep : entry.getValue()) {
                reverseDeps.computeIfAbsent(dep, k -> new ArrayList<>()).add(entry.getKey());
            }
        }

        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String node = ready.poll();
            order.add(node);

            for (String dependent : reverseDeps.getOrDefault(node, List.of())) {
                int newDegree = inDegree.get(dependent) - 1;
                inDegree.put(dependent, newDegree);
                if (newDegree == 0) {
                    ready.add(dependent);
                }
            }
        }

        if (order.size() != nodeIds.size()) {
            throw new IllegalStateException(
                    "Cycle detected in DAG! Processed " + order.size() +
                            " of " + nodeIds.size() + " nodes.");
        }

        return order;
    }

    /**
     * Shutdown the DAG executor pool.
     */
    public void shutdown() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
