package jobscheduler.distributed;

import jobscheduler.dag.DagJobScheduler;
import jobscheduler.distributed.model.Job;
import jobscheduler.distributed.node.DistributedSchedulerNode;
import jobscheduler.distributed.queue.RedisJobQueue;
import jobscheduler.distributed.repository.InMemoryJobRepository;
import jobscheduler.distributed.repository.JobRepository;
import jobscheduler.distributed.service.JobService;
import jobscheduler.distributed.watchdog.WatchdogService;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * Driver demonstrating the distributed job scheduler.
 *
 * Bootstraps:
 * - 3 DistributedSchedulerNode instances (simulating a cluster)
 * - 1 WatchdogService
 * - 1 JobService (client API)
 *
 * Demonstrates:
 * 1. One-time job (claimed by exactly one node via CAS)
 * 2. Recurring job (re-enqueued after each execution)
 * 3. Cancelled job (removed from Redis before execution)
 * 4. Failing job with exponential backoff retry
 * 5. DAG execution (reuses L5 DagJobScheduler)
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║   Distributed Job Scheduler — Staff-Level Demo          ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝\n");

        // ═══════════════════════════════════════════════════════════════
        // Infrastructure Setup
        // ═══════════════════════════════════════════════════════════════

        // Redis connection pool (ensure Redis is running on localhost:6379)
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6379);

        // Shared in-memory DB (simulates Postgres)
        JobRepository repository = new InMemoryJobRepository();

        // Redis-backed queue
        RedisJobQueue redisQueue = new RedisJobQueue(jedisPool);

        // Client API
        JobService jobService = new JobService(repository, redisQueue);

        // ═══════════════════════════════════════════════════════════════
        // Cluster Bootstrap — 3 Nodes + 1 Watchdog
        // ═══════════════════════════════════════════════════════════════

        System.out.println("── Starting 3-node cluster + watchdog ──\n");

        DistributedSchedulerNode node1 = new DistributedSchedulerNode(repository, redisQueue, 2);
        DistributedSchedulerNode node2 = new DistributedSchedulerNode(repository, redisQueue, 2);
        DistributedSchedulerNode node3 = new DistributedSchedulerNode(repository, redisQueue, 2);

        WatchdogService watchdog = new WatchdogService(repository, redisQueue);

        node1.start();
        node2.start();
        node3.start();
        watchdog.start();

        // ═══════════════════════════════════════════════════════════════
        // Demo 1: One-time Job
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Demo 1: One-Time Job ──");
        jobService.schedule("email-welcome",
                "PrintTask", "{\"to\":\"user@example.com\"}", 500, 5);

        // ═══════════════════════════════════════════════════════════════
        // Demo 2: Recurring Job
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Demo 2: Recurring Job ──");
        jobService.scheduleRecurring("health-check",
                "HeartbeatTask", "{\"service\":\"api-gateway\"}", 300, 2000, 5);

        // ═══════════════════════════════════════════════════════════════
        // Demo 3: Cancelled Job
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Demo 3: Cancelled Job ──");
        Job cancelledJob = jobService.schedule("report-gen",
                "SlowTask", "{\"report\":\"monthly\"}", 2000, 10);
        jobService.cancel(cancelledJob.getId());

        // ═══════════════════════════════════════════════════════════════
        // Demo 4: Failing Job with Retry
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Demo 4: Failing Job with Retry ──");
        jobService.scheduleWithRetry("data-sync",
                "FailingTask", "{\"source\":\"legacy-db\"}", 800, 3, 3);

        // ═══════════════════════════════════════════════════════════════
        // Let scheduler run for 10 seconds
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n[Main] Waiting 12 seconds for jobs to execute...\n");
        Thread.sleep(12_000);

        // ═══════════════════════════════════════════════════════════════
        // Demo 5: DAG Execution (reuses L5 DagJobScheduler)
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Demo 5: DAG Execution (L5 reuse) ──");

        Map<String, Runnable> nodes = new LinkedHashMap<>();
        nodes.put("Extract",   () -> simulateWork("Extract",   300));
        nodes.put("Transform", () -> simulateWork("Transform", 400));
        nodes.put("Validate",  () -> simulateWork("Validate",  200));
        nodes.put("Load",      () -> simulateWork("Load",      150));
        nodes.put("Notify",    () -> simulateWork("Notify",    100));

        // ETL Pipeline: Extract → Transform → Load
        //               Extract → Validate  → Load
        //               Load → Notify
        Map<String, List<String>> deps = new HashMap<>();
        deps.put("Transform", List.of("Extract"));
        deps.put("Validate",  List.of("Extract"));
        deps.put("Load",      List.of("Transform", "Validate"));
        deps.put("Notify",    List.of("Load"));

        DagJobScheduler dagScheduler = new DagJobScheduler(3);
        dagScheduler.executeDag(nodes, deps);
        dagScheduler.shutdown();

        // ═══════════════════════════════════════════════════════════════
        // Shutdown
        // ═══════════════════════════════════════════════════════════════

        System.out.println("\n── Shutting down cluster ──");
        watchdog.shutdown();
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();

        jedisPool.close();

        System.out.println("\n╔══════════════════════════════════════════════════════════╗");
        System.out.println("║   All distributed demos completed successfully!         ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
    }

    private static void simulateWork(String name, long durationMs) {
        try {
            System.out.printf("    [DAG %s] Working for %dms...%n", name, durationMs);
            Thread.sleep(durationMs);
            System.out.printf("    [DAG %s] Done!%n", name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
