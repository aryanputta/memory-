using System.Collections.Generic;

namespace MercuryCache.ML
{
    /// <summary>
    /// Feature schema shared between the C# training pipeline and the C++
    /// ONNX inference path.  Both sides must use this exact field ordering.
    /// </summary>
    public static class FeatureSchema
    {
        public static readonly IReadOnlyList<string> Features = new[]
        {
            "incoming_est_freq_1m",
            "incoming_est_freq_10m",
            "victim_est_freq_1m",
            "victim_est_freq_10m",
            "incoming_recency_ms",
            "victim_recency_ms",
            "incoming_size_bytes",
            "victim_size_bytes",
            "shard_pressure_ratio",
            "tenant_pressure_ratio",
            "key_hotness_score",
            "cache_hit_rate_1m",
            "db_fallback_rate_1m",
            "ttl_ms",
            "write_rate_1m",
            "read_burst_score",
            "workload_class_id"
        };

        public const int FeatureCount = 17;
    }

    /// <summary>
    /// One training example for the admission model.
    /// Label: 1 = admitting the incoming item improved future hit rate; 0 = no.
    /// </summary>
    public class AdmissionTrainingExample
    {
        public float[] Features { get; set; } = new float[FeatureSchema.FeatureCount];
        public int     Label    { get; set; }  // 0 or 1
        public string  TraceId  { get; set; } = "";
    }
}
