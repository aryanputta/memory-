using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace MercuryCache.Cluster
{
    // ── GET ──────────────────────────────────────────────────────────────────
    public class GetCacheResponse
    {
        [JsonPropertyName("key")]
        public string Key { get; set; } = "";

        [JsonPropertyName("value")]
        public string? Value { get; set; }

        [JsonPropertyName("source")]
        public string Source { get; set; } = "miss";

        [JsonPropertyName("ttlRemainingMs")]
        public long TtlRemainingMs { get; set; }

        [JsonPropertyName("version")]
        public long Version { get; set; }

        [JsonPropertyName("stale")]
        public bool Stale { get; set; }

        [JsonPropertyName("found")]
        public bool Found { get; set; }

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── PUT ──────────────────────────────────────────────────────────────────
    public class PutCacheRequest
    {
        [JsonPropertyName("value")]
        public string Value { get; set; } = "";

        [JsonPropertyName("ttlMs")]
        public long TtlMs { get; set; } = 300_000;

        [JsonPropertyName("writeMode")]
        public string WriteMode { get; set; } = "write_through";

        [JsonPropertyName("version")]
        public long Version { get; set; }
    }

    public class PutCacheResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; set; } = "";

        [JsonPropertyName("writtenTo")]
        public List<string> WrittenTo { get; set; } = new();

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── DELETE ────────────────────────────────────────────────────────────────
    public class DeleteCacheResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; set; } = "";

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── BULK GET ──────────────────────────────────────────────────────────────
    public class BulkGetRequest
    {
        [JsonPropertyName("namespace")]
        public string Namespace { get; set; } = "";

        [JsonPropertyName("keys")]
        public List<string> Keys { get; set; } = new();

        [JsonPropertyName("consistency")]
        public string Consistency { get; set; } = "eventual";
    }

    public class BulkGetItem
    {
        [JsonPropertyName("key")]
        public string Key { get; set; } = "";

        [JsonPropertyName("found")]
        public bool Found { get; set; }

        [JsonPropertyName("source")]
        public string? Source { get; set; }

        [JsonPropertyName("value")]
        public string? Value { get; set; }

        [JsonPropertyName("stale")]
        public bool Stale { get; set; }
    }

    public class BulkGetResponse
    {
        [JsonPropertyName("items")]
        public List<BulkGetItem> Items { get; set; } = new();

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── ADMIN: REBALANCE ──────────────────────────────────────────────────────
    public class RebalanceRequest
    {
        [JsonPropertyName("reason")]
        public string Reason { get; set; } = "";
    }

    public class RebalanceResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; set; } = "";

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── ADMIN: FAILOVER ───────────────────────────────────────────────────────
    public class FailoverRequest
    {
        [JsonPropertyName("shardId")]
        public int ShardId { get; set; }

        [JsonPropertyName("promoteReplicaNodeId")]
        public string PromoteReplicaNodeId { get; set; } = "";
    }

    public class FailoverResponse
    {
        [JsonPropertyName("status")]
        public string Status { get; set; } = "";

        [JsonPropertyName("traceId")]
        public string TraceId { get; set; } = "";
    }

    // ── ADMIN: METRICS ────────────────────────────────────────────────────────
    public class ClusterMetricsResponse
    {
        [JsonPropertyName("clusterHitRate")]
        public double ClusterHitRate { get; set; }

        [JsonPropertyName("p50Ms")]
        public double P50Ms { get; set; }

        [JsonPropertyName("p95Ms")]
        public double P95Ms { get; set; }

        [JsonPropertyName("p99Ms")]
        public double P99Ms { get; set; }

        [JsonPropertyName("dbFallbackRate")]
        public double DbFallbackRate { get; set; }

        [JsonPropertyName("staleReadRate")]
        public double StaleReadRate { get; set; }

        [JsonPropertyName("rebalanceInProgress")]
        public bool RebalanceInProgress { get; set; }

        [JsonPropertyName("totalRequests")]
        public long TotalRequests { get; set; }

        [JsonPropertyName("hotKeyCount")]
        public int HotKeyCount { get; set; }
    }
}
