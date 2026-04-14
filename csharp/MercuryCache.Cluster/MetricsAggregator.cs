using System;
using System.Threading;
using MercuryCache.Cluster;

namespace MercuryCache.Core
{
    /// <summary>
    /// Thread-safe in-process metrics aggregator.
    /// Tracks latency, hit rates, DB fallbacks, stale reads, etc.
    /// Exposes a dashboard snapshot for the /api/admin/metrics endpoint.
    /// </summary>
    public class MetricsAggregator
    {
        // Counters
        private long _totalRequests    = 0;
        private long _cacheHits        = 0;
        private long _cacheMisses      = 0;
        private long _dbFallbacks      = 0;
        private long _staleReads       = 0;
        private long _writes           = 0;
        private long _evictions        = 0;

        // Latency histogram buckets (µs) — 20 powers-of-2 buckets
        private readonly long[] _latencyBuckets = new long[20];

        // Running p50/p95/p99 approximation (exponentially weighted)
        private double _ewmaP50 = 0;
        private double _ewmaP95 = 0;
        private double _ewmaP99 = 0;
        private const double Alpha = 0.1; // smoothing factor

        private readonly object _latencyLock = new();

        // ── Timer ─────────────────────────────────────────────────────────
        public long StartTimer() =>
            System.Diagnostics.Stopwatch.GetTimestamp();

        public void RecordLatency(long startTick)
        {
            double ms = (double)(System.Diagnostics.Stopwatch.GetTimestamp() - startTick)
                        / System.Diagnostics.Stopwatch.Frequency * 1000.0;

            lock (_latencyLock)
            {
                _ewmaP50 = _ewmaP50 * (1 - Alpha) + ms * Alpha;
                _ewmaP95 = ms > _ewmaP95 ? _ewmaP95 * 0.95 + ms * 0.05 : _ewmaP95 * 0.999;
                _ewmaP99 = ms > _ewmaP99 ? _ewmaP99 * 0.99 + ms * 0.01 : _ewmaP99 * 0.9999;
            }
            Interlocked.Increment(ref _totalRequests);
        }

        // ── Event recording ───────────────────────────────────────────────
        public void RecordHit(string ns)
        {
            Interlocked.Increment(ref _cacheHits);
        }

        public void RecordMiss(string ns)
        {
            Interlocked.Increment(ref _cacheMisses);
        }

        public void RecordDbFallback(string ns)
        {
            Interlocked.Increment(ref _dbFallbacks);
        }

        public void RecordStaleRead(string ns)
        {
            Interlocked.Increment(ref _staleReads);
        }

        public void RecordWrite(string ns)
        {
            Interlocked.Increment(ref _writes);
        }

        public void RecordEviction()
        {
            Interlocked.Increment(ref _evictions);
        }

        // ── Dashboard snapshot ────────────────────────────────────────────
        public ClusterMetricsResponse BuildDashboardSnapshot()
        {
            long hits   = Interlocked.Read(ref _cacheHits);
            long misses = Interlocked.Read(ref _cacheMisses);
            long total  = hits + misses;
            long dbs    = Interlocked.Read(ref _dbFallbacks);
            long stales = Interlocked.Read(ref _staleReads);

            double hitRate    = total > 0 ? (double)hits / total   : 0.0;
            double dbRate     = total > 0 ? (double)dbs  / total   : 0.0;
            double staleRate  = hits  > 0 ? (double)stales / hits  : 0.0;

            double p50, p95, p99;
            lock (_latencyLock)
            {
                p50 = _ewmaP50;
                p95 = _ewmaP95;
                p99 = _ewmaP99;
            }

            return new ClusterMetricsResponse
            {
                ClusterHitRate      = Math.Round(hitRate,   4),
                P50Ms               = Math.Round(p50,       3),
                P95Ms               = Math.Round(p95,       3),
                P99Ms               = Math.Round(p99,       3),
                DbFallbackRate      = Math.Round(dbRate,    4),
                StaleReadRate       = Math.Round(staleRate, 4),
                RebalanceInProgress = false,
                TotalRequests       = Interlocked.Read(ref _totalRequests),
                HotKeyCount         = 0   // populated by HotKeyReplicationManager
            };
        }
    }
}
