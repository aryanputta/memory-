using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MercuryCache.Core;

namespace MercuryCache.Cluster
{
    /// <summary>
    /// Tracks live cluster membership using periodic heartbeat checks.
    /// On node failure, removes the node from the consistent-hash ring and
    /// triggers a rebalance event.
    /// </summary>
    public class ClusterMembershipService
    {
        private readonly ConsistentHashRing     _ring;
        private readonly CacheNodeClientFactory _nodeFactory;
        private readonly ILogger<ClusterMembershipService> _log;

        private readonly Dictionary<string, NodeState> _nodeStates = new();
        private readonly object _lock = new();

        private CancellationTokenSource? _heartbeatCts;

        public event Action<string>? NodeJoined;
        public event Action<string>? NodeLeft;

        public ClusterMembershipService(
            ConsistentHashRing ring,
            CacheNodeClientFactory nodeFactory,
            ILogger<ClusterMembershipService> log)
        {
            _ring        = ring;
            _nodeFactory = nodeFactory;
            _log         = log;
        }

        public void RegisterNode(string address)
        {
            lock (_lock)
            {
                if (_nodeStates.ContainsKey(address)) return;
                _nodeStates[address] = new NodeState { Address = address, Healthy = true };
                _ring.AddNode(address);
                _log.LogInformation("Node {Node} registered", address);
                NodeJoined?.Invoke(address);
            }
        }

        public void DeregisterNode(string address)
        {
            lock (_lock)
            {
                _nodeStates.Remove(address);
                _ring.RemoveNode(address);
                _log.LogInformation("Node {Node} deregistered", address);
                NodeLeft?.Invoke(address);
            }
        }

        public async Task<List<NodeState>> GetActiveNodesAsync()
        {
            lock (_lock)
            {
                return _nodeStates.Values
                    .Where(n => n.Healthy)
                    .ToList();
            }
        }

        /// <summary>Start background heartbeat loop (every 10 seconds).</summary>
        public void StartHeartbeatLoop(CancellationToken ct)
        {
            _heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _ = Task.Run(() => HeartbeatLoopAsync(_heartbeatCts.Token));
        }

        private async Task HeartbeatLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct);

                List<string> nodes;
                lock (_lock) { nodes = new List<string>(_nodeStates.Keys); }

                foreach (var address in nodes)
                {
                    try
                    {
                        var client = _nodeFactory.GetClient(address);
                        var resp   = await client.HeartbeatAsync("hb", ct);
                        lock (_lock)
                        {
                            if (_nodeStates.TryGetValue(address, out var state))
                            {
                                bool wasUnhealthy = !state.Healthy;
                                state.Healthy      = resp.Healthy;
                                state.CpuUtil      = resp.CpuUtil;
                                state.MemUtil      = resp.MemUtil;
                                state.ItemCount    = resp.ItemCount;
                                state.BytesUsed    = resp.BytesUsed;
                                state.LastSeen     = DateTime.UtcNow;

                                if (wasUnhealthy && resp.Healthy)
                                {
                                    _log.LogInformation("Node {Node} recovered", address);
                                    _ring.AddNode(address);
                                    NodeJoined?.Invoke(address);
                                }
                            }
                        }
                    }
                    catch (Exception)
                    {
                        lock (_lock)
                        {
                            if (_nodeStates.TryGetValue(address, out var state))
                            {
                                if (state.Healthy)
                                {
                                    state.Healthy = false;
                                    _log.LogWarning("Node {Node} is DOWN — removing from ring", address);
                                    _ring.RemoveNode(address);
                                    NodeLeft?.Invoke(address);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public class NodeState
    {
        public string   Address   { get; set; } = "";
        public bool     Healthy   { get; set; } = true;
        public double   CpuUtil   { get; set; }
        public double   MemUtil   { get; set; }
        public long     ItemCount { get; set; }
        public long     BytesUsed { get; set; }
        public DateTime LastSeen  { get; set; } = DateTime.UtcNow;
    }
}
