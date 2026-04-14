using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MercuryCache.Cluster;
using MercuryCache.Core;

namespace MercuryCache.Api.Controllers
{
    [ApiController]
    [Route("api/admin")]
    public class AdminController : ControllerBase
    {
        private readonly RequestCoordinator      _coordinator;
        private readonly MetricsAggregator       _metrics;
        private readonly ClusterMembershipService _membership;
        private readonly ReplicationCoordinator  _replication;

        public AdminController(
            RequestCoordinator coordinator,
            MetricsAggregator metrics,
            ClusterMembershipService membership,
            ReplicationCoordinator replication)
        {
            _coordinator  = coordinator;
            _metrics      = metrics;
            _membership   = membership;
            _replication  = replication;
        }

        /// <summary>
        /// POST /api/admin/rebalance — trigger consistent-hash ring rebalance
        /// </summary>
        [HttpPost("rebalance")]
        public async Task<IActionResult> Rebalance([FromBody] RebalanceRequest req)
        {
            string traceId = GenerateTraceId();
            await _coordinator.TriggerRebalanceAsync(req.Reason, traceId);
            return Ok(new RebalanceResponse { Status = "started", TraceId = traceId });
        }

        /// <summary>
        /// POST /api/admin/failover — promote a replica to primary for a shard
        /// </summary>
        [HttpPost("failover")]
        public async Task<IActionResult> Failover([FromBody] FailoverRequest req)
        {
            string traceId = GenerateTraceId();
            await _replication.PromoteReplicaAsync(req.ShardId,
                                                    req.PromoteReplicaNodeId,
                                                    traceId);
            return Ok(new FailoverResponse { Status = "promoted", TraceId = traceId });
        }

        /// <summary>
        /// GET /api/admin/metrics — cluster-wide aggregated metrics
        /// </summary>
        [HttpGet("metrics")]
        public IActionResult GetMetrics()
        {
            var snapshot = _metrics.BuildDashboardSnapshot();
            return Ok(snapshot);
        }

        /// <summary>
        /// GET /api/admin/nodes — list active cluster members
        /// </summary>
        [HttpGet("nodes")]
        public async Task<IActionResult> GetNodes()
        {
            var nodes = await _membership.GetActiveNodesAsync();
            return Ok(nodes);
        }

        /// <summary>
        /// GET /metrics — Prometheus text-format metrics endpoint.
        ///
        /// Exposes the same counters that MetricsAggregator tracks internally
        /// so Prometheus can scrape the C# API gateway alongside C++ cache nodes.
        ///
        /// Format: https://prometheus.io/docs/instrumenting/exposition_formats/
        /// </summary>
        [HttpGet("/metrics")]
        [Produces("text/plain")]
        public IActionResult PrometheusMetrics()
        {
            var s  = _metrics.BuildDashboardSnapshot();
            var sb = new StringBuilder(512);

            // ── Helpers ───────────────────────────────────────────────────
            void Gauge(string name, string help, double value, string? labels = null)
            {
                sb.AppendLine($"# HELP {name} {help}");
                sb.AppendLine($"# TYPE {name} gauge");
                sb.AppendLine(labels != null
                    ? $"{name}{{{labels}}} {value:G}"
                    : $"{name} {value:G}");
            }
            void Counter(string name, string help, double value)
            {
                sb.AppendLine($"# HELP {name} {help}");
                sb.AppendLine($"# TYPE {name} counter");
                sb.AppendLine($"{name}_total {value:G}");
            }

            // ── Cache efficiency ──────────────────────────────────────────
            Gauge("mercury_cache_hit_rate",
                  "Fraction of requests served from cache (0-1)",
                  s.ClusterHitRate);

            Gauge("mercury_db_fallback_rate",
                  "Fraction of requests that fell back to the backing store (0-1)",
                  s.DbFallbackRate);

            Gauge("mercury_stale_read_rate",
                  "Fraction of cache hits that returned a stale value (0-1)",
                  s.StaleReadRate);

            // ── Latency ───────────────────────────────────────────────────
            Gauge("mercury_request_latency_ms",
                  "Estimated request latency (milliseconds)",
                  s.P50Ms, "quantile=\"0.5\"");
            Gauge("mercury_request_latency_ms",
                  "Estimated request latency (milliseconds)",
                  s.P95Ms, "quantile=\"0.95\"");
            Gauge("mercury_request_latency_ms",
                  "Estimated request latency (milliseconds)",
                  s.P99Ms, "quantile=\"0.99\"");

            // ── Request volume ────────────────────────────────────────────
            Counter("mercury_requests",
                    "Total requests processed by the API gateway",
                    s.TotalRequests);

            // ── Cluster state ─────────────────────────────────────────────
            Gauge("mercury_hot_key_count",
                  "Number of keys currently boosted with extra replicas",
                  s.HotKeyCount);

            Gauge("mercury_rebalance_in_progress",
                  "1 if a ring rebalance is currently running, 0 otherwise",
                  s.RebalanceInProgress ? 1 : 0);

            return Content(sb.ToString(), "text/plain; version=0.0.4; charset=utf-8");
        }

        // ----------------------------------------------------------------
        private static string GenerateTraceId()
            => $"mercury-{Guid.NewGuid():N}"[..24];
    }
}
