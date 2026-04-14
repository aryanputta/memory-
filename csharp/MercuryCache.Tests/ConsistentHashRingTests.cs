using System.Collections.Generic;
using System.Linq;
using MercuryCache.Core;
using Xunit;

namespace MercuryCache.Tests
{
    public class ConsistentHashRingTests
    {
        [Fact]
        public void AddNode_IncreasesNodeCount()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            Assert.Equal(1, ring.NodeCount);
        }

        [Fact]
        public void AddNode_Idempotent_DoesNotDuplicateCount()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            ring.AddNode("node1:50051"); // duplicate
            Assert.Equal(1, ring.NodeCount);
        }

        [Fact]
        public void RemoveNode_DecreasesNodeCount()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.RemoveNode("node1:50051");
            Assert.Equal(1, ring.NodeCount);
        }

        [Fact]
        public void ResolvePrimary_ReturnsNullOnEmptyRing()
        {
            var ring = new ConsistentHashRing();
            Assert.Null(ring.ResolvePrimary("ns", "key"));
        }

        [Fact]
        public void ResolvePrimary_ReturnsSingleNodeWhenOnlyOne()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            var result = ring.ResolvePrimary("ns", "mykey");
            Assert.Equal("node1:50051", result);
        }

        [Fact]
        public void ResolvePrimary_AlwaysReturnsRegisteredNode()
        {
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var nodes = new HashSet<string> { "node1:50051", "node2:50052", "node3:50053" };

            for (int i = 0; i < 1000; i++)
            {
                var node = ring.ResolvePrimary("ns", $"key{i}");
                Assert.NotNull(node);
                Assert.Contains(node, nodes);
            }
        }

        [Fact]
        public void ResolvePrimary_IsStableForSameKey()
        {
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");

            var first  = ring.ResolvePrimary("orders", "order-42");
            var second = ring.ResolvePrimary("orders", "order-42");
            Assert.Equal(first, second);
        }

        [Fact]
        public void ResolveReplicas_ReturnsCorrectCount()
        {
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var replicas = ring.ResolveReplicas("ns", "key", replicationFactor: 2);
            Assert.Equal(2, replicas.Count);
        }

        [Fact]
        public void ResolveReplicas_NoDuplicateNodes()
        {
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var replicas = ring.ResolveReplicas("ns", "key", replicationFactor: 3);
            Assert.Equal(replicas.Count, replicas.Distinct().Count());
        }

        [Fact]
        public void ResolveReplicas_CappedAtNodeCount()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");

            // Ask for 5 replicas but only 2 nodes exist
            var replicas = ring.ResolveReplicas("ns", "key", replicationFactor: 5);
            Assert.True(replicas.Count <= 2);
        }

        [Fact]
        public void RemoveNode_KeysRerouted_ToRemainingNodes()
        {
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var before = ring.ResolvePrimary("ns", "moved-key");
            ring.RemoveNode(before!);
            var after = ring.ResolvePrimary("ns", "moved-key");

            Assert.NotNull(after);
            Assert.NotEqual(before, after);
        }

        [Fact]
        public void GetNodes_ReturnsAllAdded()
        {
            var ring = new ConsistentHashRing(10);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var nodes = ring.GetNodes().ToList();
            Assert.Equal(3, nodes.Count);
            Assert.Contains("node1:50051", nodes);
            Assert.Contains("node2:50052", nodes);
            Assert.Contains("node3:50053", nodes);
        }

        [Fact]
        public void VirtualNodes_ProduceEvenDistribution()
        {
            // With 150 virtual nodes each, the ring should distribute 1000 random
            // keys reasonably evenly — no node should own more than 60% of keys.
            var ring = new ConsistentHashRing(150);
            ring.AddNode("node1:50051");
            ring.AddNode("node2:50052");
            ring.AddNode("node3:50053");

            var counts = new Dictionary<string, int>
            {
                ["node1:50051"] = 0,
                ["node2:50052"] = 0,
                ["node3:50053"] = 0,
            };

            for (int i = 0; i < 1000; i++)
            {
                var node = ring.ResolvePrimary("ns", $"key-{i}-{i * 37}")!;
                counts[node]++;
            }

            foreach (var kv in counts)
                Assert.True(kv.Value < 600, $"{kv.Key} owns {kv.Value}/1000 keys — too skewed");
        }
    }
}
