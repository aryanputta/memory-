# MercuryCache++

**Amazon-style distributed caching system with C++ core engine and C# service layer.**

Inspired by Amazon DynamoDB Accelerator (DAX), AWS ElastiCache integration patterns,
and Amazon Builders' Library caching guidance.

---

## Architecture

```
Clients (REST)
      │
      ▼
C# API Gateway (ASP.NET Core 8)
      │  gRPC
      ├─────────► C++ Cache Node 0  (port 50051)
      ├─────────► C++ Cache Node 1  (port 50052)
      └─────────► C++ Cache Node 2  (port 50053)
                        │
                  PostgreSQL (source of truth)

Observability: Prometheus + Grafana
```

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Cache engine | C++ 17 | Microsecond-level latency, no GC pauses |
| Service layer | C# ASP.NET Core 8 | Amazon SDE stack, async gRPC |
| Transport | gRPC (protobuf) | Strongly typed, low-overhead |
| Backing store | PostgreSQL | ACID source of truth |
| ML admission | XGBoost → ONNX Runtime | Workload-adaptive eviction |
| Observability | Prometheus + Grafana | Industry-standard metrics |

---

## Key Features

### Cache Engine (C++)
- **Eviction:** LRU, LFU, **CLOCK-Pro** (ghost entries, scan-resistant)
- **Admission:** TinyLFU (Count-Min Sketch + Bloom doorkeeper)
- **ML gate:** XGBoost classifier via ONNX Runtime
- **Concurrency:** striped `shared_mutex` + lock-free read path
- **Single-flight:** thundering-herd protection per key

### Service Layer (C#)
- **Routing:** Consistent hashing ring (150 virtual nodes per physical)
- **Write modes:** write-through, write-around, stale-while-revalidate
- **Resilience:** circuit breaker, retry with exponential backoff, timeout budget
- **Replication:** async primary → replica, failover promotion
- **Hot keys:** dynamic extra replicas for keys exceeding 500 QPS
- **Write coalescing:** 50ms batch window reduces DB writes 5–10x

---

## Quick Start

### Requirements
- Docker ≥ 24 and Docker Compose ≥ 2.20
- 4 GB RAM minimum

```bash
# 1. Clone and start the full cluster
cd infra
docker compose up -d

# 2. Wait for health checks (≈ 30s)
docker compose ps

# 3. Test the API
curl "http://localhost:8080/api/cache/catalog/item:1"
curl -X PUT "http://localhost:8080/api/cache/catalog/item:1" \
     -H "Content-Type: application/json" \
     -d '{"value":"{\"name\":\"Product 1\"}","ttlMs":300000,"writeMode":"write_through","version":1}'

# 4. Run a benchmark
docker compose run --rm --profile bench benchmark \
    --workload zipf --rps 1000 --duration 60

# 5. View metrics
open http://localhost:3000   # Grafana (admin / mercury)
open http://localhost:9090   # Prometheus
```

---

## Build C++ Locally

```bash
cd cpp
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
make -j$(nproc)

# Run tests
./mercury_tests

# Start a standalone node
./cache_node_server 0.0.0.0:50051 node-dev 512
```

---

## Build C# Locally

```bash
cd csharp
dotnet restore MercuryCache.sln
dotnet build   MercuryCache.sln
dotnet run --project MercuryCache.Api
# API available at http://localhost:8080
```

---

## Train ML Admission Model

```bash
cd ml/training
pip install xgboost scikit-learn skl2onnx
python train_admission_model.py --n_samples 200000
# Exports to: ml/exported_models/admission_model.onnx
```

---

## Run Benchmarks

```bash
cd benchmarking
pip install -r requirements.txt

# Workload A: Zipf read-heavy
python runner.py --workload zipf --rps 5000 --duration 60

# Workload B: Flash sale
python runner.py --workload flash_sale --rps 5000 --duration 30

# Workload D: Node failure injection
python runner.py --workload failure --rps 2000 --duration 60

# Analyse results
python analysis.py results/ --plot
```

---

## Amazon Interview Alignment

| Interview topic | How this project covers it |
|----------------|---------------------------|
| LRU / LFU data structures | `LruEvictionPolicy.h`, `LfuEvictionPolicy.h` |
| Distributed caching | Multi-node cluster, consistent hashing, replication |
| DAX / ElastiCache | Architecture directly mirrors both systems |
| Cache-aside, write-through | Both implemented with benchmarks |
| Thundering herd | `SingleFlightTable` (C++) + `SingleFlightManager` (C#) |
| Consistent hashing | `ConsistentHashRing.cs` — 150 virtual nodes |
| Circuit breaker | `CircuitBreaker.cs` — 3-state machine |
| Tail latency | p50/p95/p99 tracked per workload |
| ML in systems | XGBoost admission gate + ONNX Runtime C++ inference |
| Builders' Library | Stale data, thundering herd, failure modes all addressed |

---

## Project Structure

```
MercuryCache++/
├── cpp/                        C++ cache engine
│   ├── cache_core/             CacheKey, CacheValue, CacheEntry, CacheStore
│   ├── eviction/               LRU, LFU, CLOCK-Pro, TinyLFU, ML admission
│   ├── concurrency/            StripedLock, SingleFlightTable
│   ├── hotkeys/                HotKeyTracker (Count-Min Sketch)
│   ├── grpc/                   cache_node.proto + gRPC server
│   ├── metrics/                Prometheus-compatible MetricsRegistry
│   ├── tests/                  Google Test unit tests
│   └── CMakeLists.txt
│
├── csharp/                     C# service layer
│   ├── MercuryCache.Api/       ASP.NET Core REST controllers
│   ├── MercuryCache.Core/      RequestCoordinator, ring, resilience, coalescing
│   ├── MercuryCache.Cluster/   gRPC node client, membership, invalidation
│   ├── MercuryCache.ML/        ONNX-based admission model service
│   └── MercuryCache.sln
│
├── ml/
│   └── training/               XGBoost training + ONNX export
│
├── benchmarking/
│   ├── workloads/              Zipf + flash-sale generators
│   ├── runner.py               Async HTTP workload runner
│   └── analysis.py             Results comparison + CDF plots
│
├── infra/
│   ├── docker-compose.yml      Full stack: nodes + API + Postgres + monitoring
│   ├── Dockerfile.cpp          Multi-stage C++ build
│   ├── Dockerfile.csharp       Multi-stage C# build
│   ├── postgres/init.sql       Schema + seed data
│   ├── prometheus/             Scrape config
│   └── grafana/                Dashboard provisioning
│
└── docs/
    ├── architecture.md
    ├── design-decisions.md     Research gaps + algorithm comparisons
    └── benchmark-report.md     Expected results + resume bullets
```

---

## References

1. **TinyLFU** — Einziger & Friedman, ACM ToS 2017: https://dl.acm.org/doi/10.1145/3149371
2. **LeCaR** — Vietri et al., USENIX HotStorage 2018: https://www.usenix.org/system/files/conference/hotstorage18/hotstorage18-paper-vietri.pdf
3. **CLOCK-Pro** — Jiang, Chen & Zhang, USENIX ATC 2005
4. **Amazon DAX** — https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.html
5. **Builders' Library** — https://aws.amazon.com/builders-library/caching-challenges-and-strategies/
6. **AWS Prescriptive Guidance** — DynamoDB + ElastiCache integration
7. **AWS Caching Best Practices** — https://aws.amazon.com/caching/best-practices/
