using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MercuryCache.Core;

namespace MercuryCache.Cluster
{
    /// <summary>
    /// Publishes cache invalidation events to all nodes in the cluster.
    ///
    /// When a write-around update occurs, the backing store is updated and all
    /// nodes are notified to evict their stale copy of the key.
    ///
    /// In a production system this would use a dedicated message bus
    /// (e.g. SNS/SQS, Kafka, or Redis Pub/Sub).  Here we fan-out directly
    /// via the gRPC Invalidate RPC for simplicity.
    /// </summary>
    public class InvalidationPublisher
    {
        private readonly ConsistentHashRing     _ring;
        private readonly CacheNodeClientFactory _nodeFactory;
        private readonly ILogger<InvalidationPublisher> _log;

        public InvalidationPublisher(
            ConsistentHashRing ring,
            CacheNodeClientFactory nodeFactory,
            ILogger<InvalidationPublisher> log)
        {
            _ring        = ring;
            _nodeFactory = nodeFactory;
            _log         = log;
        }

        /// <summary>
        /// Invalidate a single key across ALL nodes in the cluster.
        /// </summary>
        public async Task PublishKeyInvalidationAsync(
            string ns, string key, long minVersion, string traceId,
            CancellationToken ct = default)
        {
            var nodes = new List<string>(_ring.GetNodes());
            var tasks = new List<Task>(nodes.Count);

            foreach (var node in nodes)
            {
                tasks.Add(InvalidateOnNodeAsync(node, ns, key, minVersion,
                                                "invalidation_event", traceId, ct));
            }
            await Task.WhenAll(tasks);
        }

        /// <summary>Flush all keys in a namespace across the cluster.</summary>
        public async Task PublishNamespaceFlushAsync(
            string ns, string traceId, CancellationToken ct = default)
        {
            // In a real impl this would stream all keys in the namespace.
            // Here we log the intent and rely on TTL expiry for cleanup.
            _log.LogWarning("[{TraceId}] Namespace flush requested for {Ns} — "
                + "relying on TTL expiry for full cleanup.", traceId, ns);
            await Task.CompletedTask;
        }

        private async Task InvalidateOnNodeAsync(
            string node, string ns, string key,
            long minVersion, string reason, string traceId,
            CancellationToken ct)
        {
            try
            {
                var client = _nodeFactory.GetClient(node);
                await client.InvalidateAsync(ns, key, minVersion, reason, traceId, ct);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex,
                    "[{TraceId}] Invalidation failed on node {Node} for {Ns}:{Key}",
                    traceId, node, ns, key);
            }
        }
    }
}
