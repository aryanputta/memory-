using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MercuryCache.Cluster;

namespace MercuryCache.Core
{
    /// <summary>
    /// Detects hot keys and dynamically adds extra read replicas for them.
    ///
    /// Motivation: AWS DAX documentation notes that high-traffic workloads can
    /// evict smaller tenants in a shared cluster.  By replicating hot keys to
    /// more nodes and routing reads across all replicas, we avoid overloading
    /// a single node and reduce latency.
    /// </summary>
    public class HotKeyReplicationManager
    {
        // key → estimated QPS in recent sliding window
        private readonly ConcurrentDictionary<string, long> _keyQps = new();

        // keys currently boosted with extra replicas
        private readonly ConcurrentDictionary<string, DateTime> _hotKeys = new();

        private readonly long   _hotThresholdQps;
        private readonly int    _extraReplicas;
        private readonly ConsistentHashRing     _ring;
        private readonly CacheNodeClientFactory _nodeFactory;
        private readonly ILogger<HotKeyReplicationManager> _log;

        public HotKeyReplicationManager(
            ConsistentHashRing ring,
            CacheNodeClientFactory nodeFactory,
            ILogger<HotKeyReplicationManager> log,
            long hotThresholdQps = 500,
            int extraReplicas    = 2)
        {
            _ring             = ring;
            _nodeFactory      = nodeFactory;
            _log              = log;
            _hotThresholdQps  = hotThresholdQps;
            _extraReplicas    = extraReplicas;
        }

        public async Task DetectAndPromoteHotKeysAsync(string ns, string key)
        {
            string composite = $"{ns}:{key}";
            long newQps = _keyQps.AddOrUpdate(composite, 1, (_, old) => old + 1);

            if (newQps >= _hotThresholdQps &&
                !_hotKeys.ContainsKey(composite))
            {
                await PromoteToHotAsync(ns, key, composite);
            }
        }

        private async Task PromoteToHotAsync(string ns, string key, string composite)
        {
            _hotKeys[composite] = DateTime.UtcNow;
            _log.LogInformation(
                "Key {Key} is HOT (QPS ≥ {Threshold}). Adding {Extra} extra replicas.",
                composite, _hotThresholdQps, _extraReplicas);

            // In a real system: fan reads across all nodes that have this key
            // Here we warm the extra nodes via WarmKey gRPC call
            var allNodes = new List<string>(_ring.GetNodes());
            var existing = _ring.ResolveReplicas(ns, key, 2);

            int added = 0;
            foreach (var node in allNodes)
            {
                if (existing.Contains(node) || added >= _extraReplicas) continue;
                try
                {
                    var client = _nodeFactory.GetClient(node);
                    // Warm the key on the extra node (value fetched from primary replica)
                    await client.WarmKeyAsync(ns, key, traceId: "hot-key-promote");
                    added++;
                    _log.LogDebug("Warmed hot key {Key} on extra node {Node}", composite, node);
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Failed to warm key {Key} on node {Node}", composite, node);
                }
            }
        }

        public async Task RemoveExtraReplicasForKeyAsync(string ns, string key)
        {
            string composite = $"{ns}:{key}";
            _hotKeys.TryRemove(composite, out _);
            _keyQps[composite] = 0;
            // In a full impl: demote extra replicas, reduce overhead
            await Task.CompletedTask;
        }

        public bool IsHot(string ns, string key)
            => _hotKeys.ContainsKey($"{ns}:{key}");

        public int HotKeyCount => _hotKeys.Count;
    }
}
