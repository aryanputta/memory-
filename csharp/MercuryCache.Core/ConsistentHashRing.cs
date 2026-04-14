using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace MercuryCache.Core
{
    /// <summary>
    /// Consistent hashing ring with virtual nodes for minimal key movement
    /// when cluster membership changes.
    ///
    /// Each physical node is mapped to <see cref="VirtualNodesPerPhysical"/>
    /// virtual points on the ring.  A key is routed to the first virtual node
    /// clockwise from its hash position.
    ///
    /// Inspired by the partitioning strategy used in distributed systems like
    /// Amazon Dynamo and AWS ElastiCache.
    /// </summary>
    public class ConsistentHashRing
    {
        private readonly SortedDictionary<uint, string> _ring  = new();
        private readonly Dictionary<string, List<uint>> _nodePoints = new();
        private readonly int _virtualNodesPerPhysical;
        private readonly object _lock = new();

        public int VirtualNodesPerPhysical => _virtualNodesPerPhysical;
        public int NodeCount { get; private set; }

        public ConsistentHashRing(int virtualNodesPerPhysical = 150)
        {
            _virtualNodesPerPhysical = virtualNodesPerPhysical;
        }

        /// <summary>Add a node to the ring.</summary>
        public void AddNode(string nodeAddress)
        {
            lock (_lock)
            {
                if (_nodePoints.ContainsKey(nodeAddress)) return;

                var points = new List<uint>(_virtualNodesPerPhysical);
                for (int i = 0; i < _virtualNodesPerPhysical; i++)
                {
                    uint hash = Hash($"{nodeAddress}#vnode{i}");
                    _ring[hash] = nodeAddress;
                    points.Add(hash);
                }
                _nodePoints[nodeAddress] = points;
                NodeCount++;
            }
        }

        /// <summary>Remove a node and redistribute its keys.</summary>
        public void RemoveNode(string nodeAddress)
        {
            lock (_lock)
            {
                if (!_nodePoints.TryGetValue(nodeAddress, out var points)) return;

                foreach (var point in points)
                    _ring.Remove(point);

                _nodePoints.Remove(nodeAddress);
                NodeCount--;
            }
        }

        /// <summary>Resolve the primary cache node for a given cache key.</summary>
        public string? ResolvePrimary(string ns, string key)
        {
            lock (_lock)
            {
                if (_ring.Count == 0) return null;
                uint hash = Hash($"{ns}:{key}");
                // Find first node clockwise
                foreach (var kvp in _ring)
                    if (kvp.Key >= hash)
                        return kvp.Value;
                // Wrap around to the first node
                foreach (var first in _ring)
                    return first.Value;
                return null;
            }
        }

        /// <summary>
        /// Resolve the primary + (replicationFactor-1) replica nodes for a key.
        /// </summary>
        public List<string> ResolveReplicas(string ns, string key, int replicationFactor = 2)
        {
            lock (_lock)
            {
                if (_ring.Count == 0) return new();

                uint hash = Hash($"{ns}:{key}");
                var result = new List<string>(replicationFactor);
                var seen   = new HashSet<string>();

                // Walk the ring clockwise
                bool wrapped = false;
                foreach (var kvp in _ring)
                {
                    if (!wrapped && kvp.Key < hash) continue;
                    if (!seen.Contains(kvp.Value))
                    {
                        seen.Add(kvp.Value);
                        result.Add(kvp.Value);
                        if (result.Count >= replicationFactor) return result;
                    }
                }
                // Wrap around
                if (result.Count < replicationFactor)
                {
                    foreach (var kvp in _ring)
                    {
                        if (!seen.Contains(kvp.Value))
                        {
                            seen.Add(kvp.Value);
                            result.Add(kvp.Value);
                            if (result.Count >= replicationFactor) break;
                        }
                    }
                }
                return result;
            }
        }

        /// <summary>Return all currently registered node addresses.</summary>
        public IEnumerable<string> GetNodes()
        {
            lock (_lock) { return new List<string>(_nodePoints.Keys); }
        }

        // -----------------------------------------------------------------------
        private static uint Hash(string input)
        {
            Span<byte> bytes = stackalloc byte[16];
            var inputBytes = Encoding.UTF8.GetBytes(input);
            MD5.HashData(inputBytes, bytes);
            return BitConverter.ToUInt32(bytes[..4]);
        }
    }
}
