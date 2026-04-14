# MercuryCache++

**High-performance distributed caching system with a C++ core engine and C# service layer.**

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

| Component | Technology |
|-----------|-----------|
| Cache engine | C++ 17 |
| Service layer | C# ASP.NET Core 8 |
| Transport | gRPC (protobuf) |
| Backing store | PostgreSQL |
| ML admission | XGBoost → ONNX Runtime |
| Observability | Prometheus + Grafana |

---

## Key Features

### Cache Engine (C++)
- **Eviction:** LRU, LFU, **CLOCK-Pro** (ghost entries, scan-resistant)
- **Admission:** TinyLFU (Count-Min Sketch + Bloom doorkeeper)
- **ML gate:** XGBoost classifier via ONNX Runtime — adapts to workload patterns
- **Concurrency:** chunked `shared_mutex` locking; ~8× write-starvation reduction under batch reads
- **Single-flight:** thundering-herd protection per key

### Service Layer (C#)
- **Routing:** Consistent hashing ring with 150 virtual nodes per physical node
- **Write modes:** write-through, write-around, stale-while-revalidate
- **Resilience:** circuit breaker (3-state), retry with exponential backoff, per-request timeout budget
- **Replication:** async primary → replica writes, failover promotion
- **Hot keys:** dynamic extra replicas + concurrent read fanout for keys exceeding 500 QPS; reduces p99 latency 3–4×
- **Write coalescing:** 50 ms batch window reduces backing-store writes 5–10×
- **Observability:** `/metrics` endpoint in Prometheus text format

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
# After a benchmark run (generates trace files):
cd ml/training
pip install xgboost scikit-learn skl2onnx
python retrain_from_traces.py
# Exports to: ml/models/admission_model.onnx
```

---

## Run Benchmarks

```bash
cd benchmarking
pip install -r requirements.txt

# Workload A: Zipf read-heavy
python runner.py --workload zipf --rps 5000 --duration 60

# Workload B: Flash sale (hot key concentration)
python runner.py --workload flash_sale --rps 5000 --duration 30

# Workload C: Node failure injection
python runner.py --workload failure --rps 2000 --duration 60

# Analyse results
python analysis.py results/ --plot
```

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
│   ├── runner.py               Async HTTP workload runner + trace collector
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
    ├── design-decisions.md
    └── benchmark-report.md
```

---

## References

1. **TinyLFU** — Einziger & Friedman, ACM ToS 2017: https://dl.acm.org/doi/10.1145/3149371
2. **LeCaR** — Vietri et al., USENIX HotStorage 2018
3. **CLOCK-Pro** — Jiang, Chen & Zhang, USENIX ATC 2005
