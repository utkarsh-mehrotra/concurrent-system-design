# 🚀 Concurrent System Design — Production-Grade Concurrency & Scheduling in Java

A growing collection of **concurrent system design** implementations in Java, built with `java.util.concurrent` — no external libraries.

Each system demonstrates real-world concurrency patterns: lock-free data structures, thread-safe scheduling, graceful shutdown, and dependency-aware execution.

> Companion to [Advanced System Design](https://github.com/utkarsh-mehrotra/advanced-system-design) — SDE3-Level LLD Systems.

---

## 📦 Systems

### 1. Job Scheduler with Parallelism
| Component | Path | Signature Pattern |
|-----------|------|-------------------|
| `ScheduledJob` | `jobscheduler/model/` | `Delayed` + `Comparable` with priority tiebreak |
| `Worker` | `jobscheduler/worker/` | `DelayQueue.take()` zero-spin dispatcher |
| `JobScheduler` | `jobscheduler/core/` | Two-tier executor: N dispatchers + M job threads |
| `DagJobScheduler` | `jobscheduler/dag/` | `CompletableFuture.allOf()` fan-in DAG execution |
| `Main` | `jobscheduler/` | Full demo: one-time, recurring, priority, cancel, DAG |

**Architecture:**
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

<!-- Add new systems here -->

---

## 🎯 Concurrency Patterns Index

| Pattern | System | Implementation |
|---------|--------|---------------|
| **DelayQueue (Zero Busy-Wait)** | Job Scheduler | `queue.take()` parks on `Condition` — no polling, no spin |
| **Lazy Cancellation (Tombstone)** | Job Scheduler | `ConcurrentHashMap.newKeySet()` — O(1) CAS add, wait-free read |
| **Two-Tier Executor** | Job Scheduler | Dispatcher threads never execute tasks; separate pool bounds parallelism |
| **Fixed-Rate Scheduling** | Job Scheduler | `executeAtMs + period` — drift-proof, decoupled from task latency |
| **CompletableFuture DAG** | Job Scheduler | `allOf().thenRunAsync()` — implicit topological ordering via fan-in |
| **Kahn's Topological Sort** | Job Scheduler | Guarantees future-build order; cycle detection via processed count |
| **Graceful 4-Phase Shutdown** | Job Scheduler | `shutdownNow()` → `shutdown()` → `awaitTermination()` → force-kill |

---

## 🔧 Running

```bash
cd solutions/java/src

# Compile (e.g., Job Scheduler)
javac $(find jobscheduler -name "*.java")

# Run
java jobscheduler.Main
```

Replace the package name with any system directory.

---

## 📋 Design Constraints

- **Java 17+** — no external libraries, only `java.util.concurrent`
- Each system is fully self-contained and independently runnable
- Inline comments explain non-obvious concurrency decisions
- Every system includes a `Main` driver demonstrating all features
