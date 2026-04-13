using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MercuryCache.Cluster;

namespace MercuryCache.Core
{
    /// <summary>
    /// Manages async replication of writes from the primary cache node to replicas.
    ///
    /// Write flow:
    ///   1. Primary node acknowledged → return to caller
    ///   2. Fire-and-forget replication to replica nodes (async)
    ///   3. On primary failure: promote replica (via PromoteReplicaAsync)
    /// </summary>
    public class ReplicationCoordinator
    {
        private readonly ConsistentHashRing      _ring;
        private readonly CacheNodeClientFactory  _nodeFactory;
        private readonly ILogger<ReplicationCoordinator> _log;

        // shard → primary node address
        private readonly Dictionary<int, string>  _shardPrimaries = new();
        private readonly Dictionary<int, string>  _shardReplicas  = new();
        private readonly object                   _shardLock      = new();

        public ReplicationCoordinator(
            ConsistentHashRing ring,
            CacheNodeClientFactory nodeFactory,
            ILogger<ReplicationCoordinator> log)
        {
            _ring        = ring;
            _nodeFactory = nodeFactory;
            _log         = log;
        }

        /// <summary>
        /// Replicate a write to all replica nodes for the given key.
        /// Non-blocking — replication lag is acceptable (eventual consistency).
        /// </summary>
        public async Task ReplicateAsync(
            string ns, string key, string value, long ttlMs, long version,
            string primaryNodeId, string traceId, CancellationToken ct = default)
        {
            var nodes = _ring.ResolveReplicas(ns, key, replicationFactor: 2);
            foreach (var node in nodes)
            {
                if (node == primaryNodeId) continue; // skip primary
                try
                {
                    var client = _nodeFactory.GetClient(node);
                    await client.ReplicateAsync(ns, key, value, ttlMs, version,
                                                primaryNodeId, traceId, ct);
                    _log.LogDebug("[{TraceId}] Replicated {Ns}:{Key} to {Node}",
                        traceId, ns, key, node);
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex,
                        "[{TraceId}] Replication to {Node} failed for {Ns}:{Key}",
                        traceId, node, ns, key);
                }
            }
        }

        /// <summary>
        /// Promote a replica to primary for the given shard.
        /// Called on primary node failure.
        /// </summary>
        public async Task PromoteReplicaAsync(
            int shardId, string replicaNodeId, string traceId,
            CancellationToken ct = default)
        {
            _log.LogWarning(
                "[{TraceId}] Promoting replica {Replica} to primary for shard {Shard}",
                traceId, replicaNodeId, shardId);

            // In a full impl: drain in-flight requests, sync replica lag,
            // update membership service, redirect new traffic to promoted node.
            lock (_shardLock)
            {
                _shardPrimaries[shardId] = replicaNodeId;
            }

            // Verify replica is healthy
            try
            {
                var client = _nodeFactory.GetClient(replicaNodeId);
                await client.HeartbeatAsync(traceId, ct);
                _log.LogInformation(
                    "[{TraceId}] Shard {Shard} primary is now {Node}",
                    traceId, shardId, replicaNodeId);
            }
            catch (Exception ex)
            {
                _log.LogError(ex,
                    "[{TraceId}] Promoted replica {Node} failed health check!",
                    traceId, replicaNodeId);
                throw;
            }
        }

        public async Task<long> GetReplicaLagAsync(
            string primaryNode, string replicaNode, string traceId,
            CancellationToken ct = default)
        {
            try
            {
                var primaryClient = _nodeFactory.GetClient(primaryNode);
                var replicaClient = _nodeFactory.GetClient(replicaNode);

                var primaryHealth = await primaryClient.HeartbeatAsync(traceId, ct);
                var replicaHealth = await replicaClient.HeartbeatAsync(traceId, ct);

                // Simple approximation: compare item counts
                return Math.Abs(primaryHealth.ItemCount - replicaHealth.ItemCount);
            }
            catch
            {
                return -1;
            }
        }
    }
}
