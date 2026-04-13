using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Linq;

namespace MercuryCache.Cluster
{
    public class ClusterHealthCheck : IHealthCheck
    {
        private readonly ClusterMembershipService _membership;

        public ClusterHealthCheck(ClusterMembershipService membership)
        {
            _membership = membership;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context, CancellationToken ct = default)
        {
            var nodes = await _membership.GetActiveNodesAsync();
            int healthy = nodes.Count(n => n.Healthy);

            if (healthy == 0)
                return HealthCheckResult.Unhealthy($"No healthy cache nodes. Total: {nodes.Count}");
            if (healthy < nodes.Count)
                return HealthCheckResult.Degraded($"Healthy: {healthy}/{nodes.Count} cache nodes");

            return HealthCheckResult.Healthy($"All {healthy} cache nodes healthy");
        }
    }
}
