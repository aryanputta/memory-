using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MercuryCache.Cluster;
using MercuryCache.Core;
using MercuryCache.Cluster;

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

        // ----------------------------------------------------------------
        private static string GenerateTraceId()
            => $"mercury-{Guid.NewGuid():N}"[..24];
    }
}
