using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using MercuryCache.Cluster;

namespace MercuryCache.Api;

/// <summary>
/// Starts the cluster membership heartbeat loop when the application starts,
/// and stops it cleanly on shutdown.
/// </summary>
public sealed class ClusterHeartbeatHostedService : IHostedService
{
    private readonly ClusterMembershipService _membership;

    public ClusterHeartbeatHostedService(ClusterMembershipService membership)
        => _membership = membership;

    public Task StartAsync(CancellationToken ct)
    {
        _membership.StartHeartbeatLoop(ct);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
