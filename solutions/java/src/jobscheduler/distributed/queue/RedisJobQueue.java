package jobscheduler.distributed.queue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis-backed distributed job queue using Sorted Sets (ZSET).
 *
 * Key: "job_scheduler:queue"
 * Score: executeAtMs (epoch millis) — natural time ordering
 * Member: jobId (UUID string)
 *
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ DESIGN TRADE-OFF: Pull Polling vs Push Notifications            │
 * │                                                                  │
 * │ We use pull-based polling (nodes poll every ~500ms). This adds   │
 * │ up to 500ms of scheduling latency but avoids the complexity of  │
 * │ Redis Pub/Sub fan-out + message loss on subscriber disconnect.   │
 * │                                                                  │
 * │ For sub-100ms latency, use Redis Streams (XREADGROUP) or        │
 * │ Pub/Sub as a wake-up signal alongside the ZSET.                 │
 * ├──────────────────────────────────────────────────────────────────┤
 * │ DESIGN TRADE-OFF: Redis AP vs DB Durability                     │
 * │                                                                  │
 * │ Redis is an ephemeral scheduling index, NOT the source of truth.│
 * │ The DB (Postgres) owns the durable job state. If Redis crashes, │
 * │ the WatchdogService can reconstruct the queue from DB records   │
 * │ where status = PENDING.                                         │
 * ├──────────────────────────────────────────────────────────────────┤
 * │ DESIGN TRADE-OFF: Priority Sharding                             │
 * │                                                                  │
 * │ Currently all priorities share one ZSET key. For strict priority │
 * │ isolation, shard into job_scheduler:queue:high and               │
 * │ job_scheduler:queue:normal. Nodes poll high-priority first.     │
 * │ Trade-off: more Redis round-trips per poll cycle.               │
 * └──────────────────────────────────────────────────────────────────┘
 */
public class RedisJobQueue {

    private static final String QUEUE_KEY = "job_scheduler:queue";

    private final JedisPool jedisPool;

    /**
     * Lua script for atomic poll-and-remove of due jobs.
     *
     * Why a Lua script instead of ZRANGEBYSCORE + ZREM in two calls?
     * Without atomicity, two nodes can both ZRANGEBYSCORE the same job
     * before either removes it — a classic TOCTOU (Time-of-Check-
     * Time-of-Use) race. The Lua script runs as a single atomic
     * operation inside Redis, eliminating this window entirely.
     *
     * Script parameters:
     *   KEYS[1] = queue key
     *   ARGV[1] = nowMs (upper bound score)
     *   ARGV[2] = batchSize (LIMIT count)
     *
     * Returns: list of jobIds that were atomically removed.
     */
    private static final String POLL_LUA_SCRIPT =
            "local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]) " +
            "if #jobs > 0 then " +
            "  redis.call('ZREM', KEYS[1], unpack(jobs)) " +
            "end " +
            "return jobs";

    public RedisJobQueue(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * Enqueue a job into the sorted set with executeAtMs as the score.
     * ZADD is idempotent — re-enqueue of the same jobId updates score.
     */
    public void enqueue(String jobId, long executeAtMs) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zadd(QUEUE_KEY, executeAtMs, jobId);
        }
    }

    /**
     * Atomically poll and remove up to batchSize jobs that are due
     * (score ≤ nowMs). Uses a Lua script for TOCTOU safety.
     *
     * @param nowMs     current epoch millis
     * @param batchSize max jobs to dequeue in one poll
     * @return list of jobIds atomically removed from the queue
     */
    @SuppressWarnings("unchecked")
    public List<String> pollDueJobs(long nowMs, int batchSize) {
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(
                    POLL_LUA_SCRIPT,
                    1,                                    // number of KEYS
                    QUEUE_KEY,                            // KEYS[1]
                    String.valueOf(nowMs),                // ARGV[1]
                    String.valueOf(batchSize)             // ARGV[2]
            );

            if (result instanceof List<?>) {
                return (List<String>) result;
            }
            return List.of();
        }
    }

    /**
     * Remove a specific job from the queue (e.g., on cancellation).
     */
    public void remove(String jobId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zrem(QUEUE_KEY, jobId);
        }
    }

    /**
     * Get current queue depth (for observability / metrics).
     */
    public long size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard(QUEUE_KEY);
        }
    }
}
