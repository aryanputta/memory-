#pragma once
// ML-Based Cache Admission Policy
// Inspired by LeCaR (USENIX HotStorage 2018) and learned admission concepts.
//
// Design:
//   - Keep LRU/LFU as the primary eviction backbone.
//   - Use an ML model at the admission boundary: score whether the incoming
//     item is worth admitting over the eviction candidate.
//   - Runtime: ONNX Runtime for C++ inference (model trained in Python/XGBoost).
//   - Fallback: TinyLFU gate if ONNX model unavailable.
//
// Feature vector matches FeatureSchema.cs in the C# ML service.

#include "TinyLfuAdmissionPolicy.h"
#include "../cache_core/CacheEntry.h"
#include "../cache_core/RequestContext.h"
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>

namespace mercury {

// Feature vector matching the training schema (17 features)
struct AdmissionFeatures {
    float incoming_est_freq_1m    = 0;
    float incoming_est_freq_10m   = 0;
    float victim_est_freq_1m      = 0;
    float victim_est_freq_10m     = 0;
    float incoming_recency_ms     = 0;
    float victim_recency_ms       = 0;
    float incoming_size_bytes     = 0;
    float victim_size_bytes       = 0;
    float shard_pressure_ratio    = 0; // current_bytes / max_bytes
    float tenant_pressure_ratio   = 0;
    float key_hotness_score       = 0;
    float cache_hit_rate_1m       = 0;
    float db_fallback_rate_1m     = 0;
    float ttl_ms                  = 0;
    float write_rate_1m           = 0;
    float read_burst_score        = 0;
    float workload_class_id       = 0; // 0=read_heavy, 1=flash_sale, 2=mixed

    std::vector<float> ToVector() const {
        return {
            incoming_est_freq_1m, incoming_est_freq_10m,
            victim_est_freq_1m,   victim_est_freq_10m,
            incoming_recency_ms,  victim_recency_ms,
            incoming_size_bytes,  victim_size_bytes,
            shard_pressure_ratio, tenant_pressure_ratio,
            key_hotness_score,    cache_hit_rate_1m,
            db_fallback_rate_1m,  ttl_ms,
            write_rate_1m,        read_burst_score,
            workload_class_id
        };
    }
};

// ---------------------------------------------------------------------------
// ONNX inference wrapper (stub — real impl links onnxruntime)
// ---------------------------------------------------------------------------
class OnnxInferenceSession {
public:
    explicit OnnxInferenceSession(const std::string& model_path)
        : model_path_(model_path), loaded_(false) {
        // In production: load via Ort::Env + Ort::Session
        // Stub: mark as not loaded; fallback to TinyLFU
        loaded_ = false; // flip to true after linking onnxruntime
    }

    bool IsLoaded() const { return loaded_; }

    // Returns probability that admitting the incoming item improves hit rate
    float Score(const std::vector<float>& features) {
        if (!loaded_) throw std::runtime_error("ONNX model not loaded");
        // Real impl: run->GetTensorMutableData<float>()[0]
        // Stub: return 0.5f (neutral)
        (void)features;
        return 0.5f;
    }

private:
    std::string model_path_;
    bool        loaded_;
};

// ---------------------------------------------------------------------------
// ML Admission Policy
// ---------------------------------------------------------------------------
class MlAdmissionPolicy {
public:
    // Threshold: score above this → admit incoming; below → reject
    static constexpr float kDefaultThreshold = 0.55f;

    MlAdmissionPolicy(const std::string& onnx_model_path,
                      TinyLfuAdmissionPolicy* tinylfu_fallback,
                      float threshold = kDefaultThreshold)
        : onnx_(onnx_model_path)
        , tinylfu_(tinylfu_fallback)
        , threshold_(threshold)
    {}

    AdmissionFeatures ExtractFeatures(const RequestContext& ctx,
                                      const CacheEntry& incoming,
                                      const CacheEntry& victim,
                                      float shard_pressure,
                                      float hit_rate_1m,
                                      float db_fallback_rate_1m,
                                      float write_rate_1m,
                                      float read_burst_score,
                                      float key_hotness) const
    {
        AdmissionFeatures f;
        f.incoming_est_freq_1m  = static_cast<float>(incoming.freq);
        f.incoming_est_freq_10m = static_cast<float>(incoming.freq);
        f.victim_est_freq_1m    = static_cast<float>(victim.freq);
        f.victim_est_freq_10m   = static_cast<float>(victim.freq);
        f.incoming_recency_ms   = static_cast<float>(ctx.now_ms - incoming.last_access_ms);
        f.victim_recency_ms     = static_cast<float>(ctx.now_ms - victim.last_access_ms);
        f.incoming_size_bytes   = static_cast<float>(incoming.cost_bytes);
        f.victim_size_bytes     = static_cast<float>(victim.cost_bytes);
        f.shard_pressure_ratio  = shard_pressure;
        f.tenant_pressure_ratio = static_cast<float>(ctx.tenant_qps) / 10000.0f;
        f.key_hotness_score     = key_hotness;
        f.cache_hit_rate_1m     = hit_rate_1m;
        f.db_fallback_rate_1m   = db_fallback_rate_1m;
        f.ttl_ms = static_cast<float>(
            incoming.value.expires_at_ms > 0
                ? incoming.value.expires_at_ms - incoming.value.created_at_ms
                : 0);
        f.write_rate_1m         = write_rate_1m;
        f.read_burst_score      = read_burst_score;
        // Map workload class string to id
        if (ctx.workload_class == "flash_sale")   f.workload_class_id = 1.0f;
        else if (ctx.workload_class == "mixed")   f.workload_class_id = 2.0f;
        else                                       f.workload_class_id = 0.0f;
        return f;
    }

    bool ShouldAdmit(const AdmissionFeatures& features,
                     const CacheEntry& incoming,
                     const CacheEntry& victim)
    {
        if (!onnx_.IsLoaded()) {
            // Fallback: TinyLFU gate
            return tinylfu_ ? tinylfu_->ShouldAdmit(incoming, victim) : true;
        }
        float score = onnx_.Score(features.ToVector());
        return score >= threshold_;
    }

private:
    OnnxInferenceSession   onnx_;
    TinyLfuAdmissionPolicy* tinylfu_; // fallback, not owned
    float                  threshold_;
};

} // namespace mercury
