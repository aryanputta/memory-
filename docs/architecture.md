# MercuryCache++ — System Architecture

## Overview

MercuryCache++ is a production-style distributed caching platform inspired by:
- **Amazon DynamoDB Accelerator (DAX)** — in-memory read acceleration that cuts
  latency from milliseconds to microseconds
- **AWS ElastiCache** — distributed cache tier with read-through patterns
- **Amazon Builders' Library** — "Caching challenges and strategies" guidance on
  consistency, stale data, thundering herd, and operational failure modes

---

## Language Responsibilities

| Layer          | Language | Role |
|----------------|----------|------|
| Cache Engine   | C++      | High-performance in-memory store, eviction, admission, gRPC server |
| Service Layer  | C# (.NET)| API gateway, routing, replication, resilience, hot-key handling |
| ML Pipeline    | Python   | Feature extraction, XGBoost training, ONNX export |
| Benchmarking   | Python   | Workload simulation, analysis |

---

## Request Flow

```
Client
  │
  ▼
C# API Gateway (ASP.NET Core, REST)
  │
  ▼
RequestCoordinator
  ├── ConsistentHashRing  ──► routes to primary C++ node
  ├── SingleFlightManager ──► deduplicates concurrent misses (thundering herd)
  ├── CircuitBreaker      ──► fast-fails if node is down
  │
  ▼  (cache hit)                     (cache miss)
GetCacheResponse                BackingStoreAdapter (PostgreSQL)
                                  │
                                  └── FillCacheAsync (cache-aside)
                                        │
                                        ▼
                                  C++ Cache Node (gRPC Put)
                                        │
                                        ▼
                                  ReplicationCoordinator
                                  (async replication to replicas)
```

---

## C++ Cache Engine Internals

### Data Structures

| Structure | Purpose |
|-----------|---------|
| `std::unordered_map<CacheKey, CacheEntry>` | O(1) lookup |
| Doubly-linked list + position map | LRU ordering |
| Frequency bucket map | LFU ordering |
| Clock ring + ghost set | CLOCK-Pro ordering |
| Count-Min Sketch | TinyLFU frequency estimation |
| Bloom Filter (doorkeeper) | TinyLFU admission pre-filter |

### Eviction Pipeline

```
[Cache Full] → TinyLFU Gate
                  │ reject (incoming freq < victim freq)
                  │ admit
                  ▼
            ML Admission Score (ONNX)
                  │ score < 0.55 → reject
                  │ score ≥ 0.55 → admit
                  ▼
            Evict victim → store incoming
```

### Write Path

```
PUT request
  │
  ├── write_through: DB write → cache write → async replica write
  └── write_around:  DB write → cache invalidate
```

---

## Consistent Hashing

- 150 virtual nodes per physical node (minimises key movement on topology change)
- MD5-based ring hash
- `ResolveReplicas(key, factor=2)` returns primary + 1 replica
- Node add/remove triggers a rebalance scan

---

## Resilience Patterns

| Pattern | Implementation |
|---------|---------------|
| Circuit Breaker | 3-state (Closed → Open → Half-Open) |
| Thundering Herd | `SingleFlightManager` — one loader per key |
| Stale-While-Revalidate | Serve stale entry + background refresh |
| Timeout Budget | 5s per request via `CancellationTokenSource` |
| Retry | Exponential backoff in `RetryPolicy` |
| Failover | `ReplicationCoordinator.PromoteReplicaAsync` |

---

## Hot Key Handling

1. `HotKeyTracker` (C++) — Count-Min Sketch with sliding window
2. Threshold: 500 QPS sustained
3. On hot key detection: fan out to extra replicas via `WarmKey` gRPC
4. `HotKeyReplicationManager` (C#) — coordinates extra replica placement
5. Prevents noisy-neighbor eviction (per AWS DAX cluster considerations)

---

## Observability

- **Prometheus** — scrapes `/metrics` from API gateway + cache nodes
- **Grafana** — pre-provisioned dashboard with hit rate, p99, DB fallback rate
- **Metrics exported:** p50/p95/p99 latency, hit rate, eviction count,
  admission rejections, stale read rate, DB fallback QPS, failover time

---

## New Concepts (Research-Driven Improvements)

### 1. CLOCK-Pro Eviction (C++)
File: `cpp/eviction/ClockProEvictionPolicy.h`

CLOCK-Pro improves on LRU by maintaining ghost entries for recently-evicted
keys. When a ghost key is re-accessed, it is promoted to "hot" on next
insertion — capturing recurrence patterns invisible to plain LRU. This is
especially valuable for Amazon catalog workloads where sequential scans
temporarily pollute the LRU queue.

Reference: Jiang, Chen & Zhang — USENIX ATC 2005

### 2. Write-Coalescing Buffer (C#)
File: `csharp/MercuryCache.Core/WriteCoalescingBuffer.cs`

Batches writes to the same key within a 50ms window, keeping only the
highest-version entry per key before flushing to PostgreSQL. Reduces DB
write amplification by up to 10x under flash-sale workloads where the same
inventory key is updated dozens of times per second.

Reference: Amazon Aurora log-structured write path; Facebook TAO write-coalescing
