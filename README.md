# 🚀 Advanced System Design Part 2 — Concurrency & Scheduling Patterns

Production-grade **Low Level Design (LLD)** implementations in Java, focusing on advanced concurrency, scheduling, and dependency orchestration patterns.

> Continuation of [Advanced System Design Part 1](https://github.com/utkarsh-mehrotra/advanced-system-design) — SDE3-Level LLD Systems.

---

## 📦 Systems

### Job Scheduler with Parallelism
| Component | Path | Signature Pattern |
|-----------|------|-------------------|
| `ScheduledJob` | `jobscheduler/model/` | `Delayed` + `Comparable` with priority tiebreak |
| `Worker` | `jobscheduler/worker/` | `DelayQueue.take()` zero-spin dispatcher |
| `JobScheduler` | `jobscheduler/core/` | Two-tier executor: N dispatchers + M job threads |
| `DagJobScheduler` | `jobscheduler/dag/` | `CompletableFuture.allOf()` fan-in DAG execution |
| `Main` | `jobscheduler/` | Full demo: one-time, recurring, priority, cancel, DAG |

---

## 🎯 Concurrency Patterns Demonstrated

| Pattern | Implementation |
|---------|---------------|
| **DelayQueue (Zero Busy-Wait)** | `queue.take()` parks on `Condition` — no polling, no spin |
| **Lazy Cancellation (Tombstone)** | `ConcurrentHashMap.newKeySet()` — O(1) CAS add, wait-free read |
| **Two-Tier Executor** | Dispatcher threads never execute tasks; separate pool bounds parallelism |
| **Fixed-Rate Scheduling** | `executeAtMs + period` — drift-proof, decoupled from task latency |
| **CompletableFuture DAG** | `allOf().thenRunAsync()` — implicit topological ordering via fan-in |
| **Kahn's Topological Sort** | Guarantees future-build order; cycle detection via processed count |
| **Graceful 4-Phase Shutdown** | `shutdownNow()` → `shutdown()` → `awaitTermination()` → force-kill |

---

## 🏗️ Architecture

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

**Why two pools?**
- Worker threads must stay parked on `take()` to drain jobs as fast as they mature
- If workers also executed tasks, a long-running task would starve the dispatch loop
- The jobExecutor is a classic fixed-pool that bounds CPU/IO parallelism

---

## 🔧 Running

```bash
cd solutions/java/src

# Compile
javac $(find jobscheduler -name "*.java")

# Run
java jobscheduler.Main
```

---

## 📋 Design Constraints

- **Java 17+** — no external libraries, only `java.util.concurrent`
- **`DelayQueue<ScheduledJob>`** for time-ordered dispatch
- **`ExecutorService`** (fixed thread pool) for parallel execution
- **`ConcurrentHashMap`-backed Set** for cancellation
- **`CompletableFuture`** composition for DAG execution
