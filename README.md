# 🚀 Concurrent System Design — Production-Grade Concurrency & Scheduling in Java

A growing collection of **concurrent system design** implementations in Java, built with `java.util.concurrent` — no frameworks, no magic.

Each system demonstrates real-world concurrency patterns: lock-free data structures, thread-safe scheduling, distributed coordination, and graceful shutdown.

> Companion to [Advanced System Design](https://github.com/utkarsh-mehrotra/advanced-system-design) — SDE3-Level LLD Systems.

---

## 📦 Systems

### 1. Job Scheduler — In-Process (L5)

Single-JVM scheduler with `DelayQueue` + two-tier executor architecture.

| Component | Path | Signature Pattern |
|-----------|------|-------------------|
| `ScheduledJob` | `jobscheduler/model/` | `Delayed` + `Comparable` with priority tiebreak |
| `Worker` | `jobscheduler/worker/` | `DelayQueue.take()` zero-spin dispatcher |
| `JobScheduler` | `jobscheduler/core/` | Two-tier executor: N dispatchers + M job threads |
| `DagJobScheduler` | `jobscheduler/dag/` | `CompletableFuture.allOf()` fan-in DAG execution |

```
┌───────────────────────────┐
│      DelayQueue           │  ← time-ordered, priority-tiebroken
└──────────┬────────────────┘
           │ take()
┌──────────▼────────────────┐
│  workerExecutor (N)       │  ← dispatcher threads (block on queue)
└──────────┬────────────────┘
           │ submit()
┌──────────▼────────────────┐
│  jobExecutor (M)          │  ← actual task execution pool
└───────────────────────────┘
```

---

### 2. Distributed Job Scheduler — Multi-Node (Staff)

Production-grade distributed layer over L5. Redis ZSET for scheduling, CAS-based job claiming, exponential backoff, heartbeat watchdog.

| Component | Path | Signature Pattern |
|-----------|------|-------------------|
| `Job` | `distributed/model/` | Persistent entity with `taskClassName` + JSON payload |
| `JobStatus` | `distributed/model/` | State machine: PENDING → RUNNING → DONE/FAILED |
| `JobRepository` | `distributed/repository/` | CAS-based persistence (simulates Postgres row locks) |
| `RedisJobQueue` | `distributed/queue/` | ZSET + atomic Lua script (ZRANGEBYSCORE + ZREM) |
| `DistributedSchedulerNode` | `distributed/node/` | Poll → CAS claim → execute → heartbeat |
| `WatchdogService` | `distributed/watchdog/` | Stale heartbeat detection → RUNNING → PENDING recovery |
| `JobService` | `distributed/service/` | Client API with dual-write (DB first, Redis second) |

```
┌─────────────────────────────────────────────────────────┐
│  Redis ZSET (job_scheduler:queue)                       │
│  score = executeAtMs                                    │
└──────┬──────────┬──────────┬────────────────────────────┘
       │ poll     │ poll     │ poll
┌──────▼───┐ ┌───▼────┐ ┌───▼────┐
│  Node-1  │ │ Node-2 │ │ Node-3 │   ← CAS claim in DB
└──────┬───┘ └───┬────┘ └───┬────┘
       │         │          │
       └────┬────┘──────────┘
            ▼
┌──────────────────────┐    ┌───────────────────┐
│  InMemory / Postgres │◄───│  WatchdogService  │
│  (source of truth)   │    │  (stale recovery) │
└──────────────────────┘    └───────────────────┘
```

<!-- Add new systems here -->

---

## 🎯 Concurrency Patterns Index

| Pattern | System | Implementation |
|---------|--------|---------------|
| **DelayQueue (Zero Busy-Wait)** | L5 Scheduler | `queue.take()` parks on `Condition` — no polling |
| **Lazy Cancellation (Tombstone)** | L5 Scheduler | `ConcurrentHashMap.newKeySet()` — O(1) CAS |
| **CompletableFuture DAG** | L5 DAG | `allOf().thenRunAsync()` — implicit topo ordering |
| **Atomic Lua Script** | Distributed | `ZRANGEBYSCORE + ZREM` in one `eval()` — no TOCTOU |
| **CAS Job Claiming** | Distributed | `compareAndSetStatus(PENDING → RUNNING)` — distributed mutex |
| **Heartbeat + Watchdog** | Distributed | 5s heartbeat, 30s stale threshold, 15s scan interval |
| **Exponential Backoff Retry** | Distributed | `2^retryCount * 1000ms` with max retry cap |
| **Dual-Write (DB → Redis)** | Distributed | DB first for durability, Redis second for scheduling |
| **At-Least-Once Delivery** | Distributed | Watchdog re-enqueues stale jobs; tasks must be idempotent |

---

## 🔧 Running

### L5 — In-Process Scheduler
```bash
cd solutions/java/src
javac $(find jobscheduler -name "*.java" -not -path "*/distributed/*")
java jobscheduler.Main
```

### Staff — Distributed Scheduler
```bash
# Start Redis
redis-server --daemonize yes

# Compile (Jedis JARs required in lib/)
cd solutions/java/src
javac -cp "../lib/*" $(find . -name "*.java")
java -cp ".:../lib/*" jobscheduler.distributed.Main
```

### Dependencies (lib/)
```
jedis-5.1.0.jar
commons-pool2-2.12.0.jar
slf4j-api-2.0.9.jar
slf4j-simple-2.0.9.jar
```

---

## 📋 Design Constraints

- **Java 17+** — `java.util.concurrent` + Jedis for Redis
- **No Spring, no Quartz** — pure concurrent Java
- Each system is fully self-contained and independently runnable
- Inline comments document non-obvious concurrency trade-offs
