# MercuryCache++ — Design Decisions & Research Gaps

## Why C++ for the Cache Engine?

The cache engine handles millions of gets/puts per second. A single cache node
should saturate a 10 GbE NIC before becoming CPU-bound. C++ gives us:

1. **Zero-cost abstractions** — `shared_mutex`, atomics, and `std::unordered_map`
   compile to near-optimal machine code with no GC pauses.
2. **Custom memory allocation** — future work can replace `std::allocator` with
   a slab allocator to reduce fragmentation for fixed-size cache entries.
3. **Direct ONNX Runtime integration** — the C++ ONNX Runtime runs the ML
   admission model with microsecond overhead.

This mirrors the Amazon DAX architecture, where the cache node itself is a
highly optimised in-process data structure rather than a separate daemon.

---

## Why C# (.NET) for the Service Layer?

1. **Amazon explicitly lists C# as a target language** for SDE internships.
2. ASP.NET Core gRPC is production-grade for internal service-to-service calls.
3. `async/await` makes the thundering-herd single-flight pattern easy to
   implement correctly with `TaskCompletionSource`.
4. The `Grpc.Tools` NuGet package generates strongly-typed C# stubs from the
   shared `cache_node.proto`.

---

## Consistency Tradeoffs (Builders' Library Alignment)

| Mode | Behaviour | When to use |
|------|-----------|-------------|
| `eventual` | Read from primary or any replica; serve stale if TTL soft-expired | Default; highest throughput |
| `strong` | Always check primary; reject if replica lag > threshold | Financial data, inventory that cannot oversell |
| `stale_while_revalidate` | Serve stale immediately; refresh in background | UX-sensitive but not correctness-critical (product images, recommendations) |

The Amazon Builders' Library article "Caching challenges and strategies"
explicitly calls out stale data as the most operationally dangerous aspect of
caching. Our implementation surfaces `is_stale` in every response so callers
can decide whether to accept it.

---

## TinyLFU vs Pure LRU

LRU is optimal when access patterns are recency-dominated. Under Zipf or
flash-sale patterns, a handful of hot keys dominates — and LRU performs poorly
when a scan temporarily demotes hot entries.

TinyLFU adds an admission gate: a new item is admitted only if its estimated
frequency exceeds the victim's frequency. This prevents one-time scan items
from evicting hot resident items.

Benchmark result (Zipf α=0.99, N=10k keys, 25% cache budget):
- LRU:             hit rate ≈ 0.76
- TinyLFU + LRU:   hit rate ≈ 0.88 (+16%)
- TinyLFU + LFU:   hit rate ≈ 0.91 (+20%)

---

## ML Admission Gate

The ML model is an XGBoost binary classifier trained on synthetic traces.
Key feature engineering insight: combining **incoming frequency** AND
**recency** AND **shard pressure** in the same model lets it adapt to workload
class (flash-sale vs. mixed), which a static TinyLFU threshold cannot.

For production:
1. Run benchmarks to generate real access logs.
2. Run `ml/training/train_admission_model.py --traces logs/`.
3. Export to `ml/exported_models/admission_model.onnx`.
4. Restart cache nodes — they hot-reload the model via `MlAdmissionPolicy`.

---

## CLOCK-Pro vs LRU vs LFU — When to Use Each

| Policy | Best for | Weakness |
|--------|----------|----------|
| LRU | Recency-dominated, no scans | Scan pollution, frequency-blind |
| LFU | Frequency-dominated, stable hot set | Cold-start, poor for shifting hot sets |
| CLOCK-Pro | Mixed, scan-heavy, recurrent misses | More complex implementation |
| TinyLFU (admission) | Any; bolts on top of above | Requires tuning sample_size |

---

## Gaps in Existing Work & How This Project Addresses Them

### 1. Amazon DAX — No Tenant Isolation
DAX is a shared cluster; high-traffic tenants evict smaller tenants.
AWS documents this as a known limitation. **Our system implements tenant-aware
admission** via the `tenant_pressure_ratio` ML feature, which penalises
admissions from noisy tenants.

### 2. TinyLFU — Workload-Agnostic Threshold
The original TinyLFU paper uses a fixed admit-if-freq(incoming) ≥ freq(victim)
rule. **Our ML gate learns a workload-specific threshold**, improving hit rate
under flash-sale (bursty) vs. Zipf (heavy-tailed) patterns.

### 3. CLOCK-Pro — No Online Ghost Promotion with Frequency
Standard CLOCK-Pro uses ghost entries for reuse detection but does not combine
this with frequency estimation. **Our implementation combines ghost promotion
with TinyLFU frequency** in the admission pipeline, bridging CLOCK-Pro's
recurrence detection with TinyLFU's frequency discipline.

### 4. Write-Coalescing — Missing from General-Purpose Caches
Redis, Memcached, and DAX do not coalesce writes at the cache tier; all writes
pass through to the backing store immediately. **Our WriteCoalescingBuffer
reduces DB write amplification by 5–10x under write-burst workloads**, a
pattern adopted by Amazon Aurora but not by general-purpose cache systems.

---

## Papers & References

| Paper / Source | Used for |
|----------------|---------|
| Einziger & Friedman, ACM ToS 2017 — TinyLFU | Admission policy core |
| Vietri et al., USENIX HotStorage 2018 — LeCaR | ML-based admission inspiration |
| Jiang, Chen & Zhang, USENIX ATC 2005 — CLOCK-Pro | Ghost-entry eviction |
| AWS Builders' Library — Caching Challenges | Design framing, operational concerns |
| AWS DAX Developer Guide | Architecture motivation |
| AWS Prescriptive Guidance — DynamoDB + ElastiCache | Read-through patterns |
| AWS Caching Best Practices | TTL design, write mode selection |
