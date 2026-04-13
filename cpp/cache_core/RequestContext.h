#pragma once
#include <string>
#include <cstdint>

namespace mercury {

// Carries per-request metadata through the C++ engine.
// Filled by the gRPC handler from incoming metadata / request fields.
struct RequestContext {
    std::string trace_id;
    int64_t     now_ms            = 0;
    bool        hot_key_suspected = false;
    std::string workload_class;   // "read_heavy" | "flash_sale" | "mixed"
    uint32_t    shard_id          = 0;
    uint64_t    recent_key_qps    = 0; // QPS observed for this key in last 60s
    uint64_t    tenant_qps        = 0; // total QPS from the issuing tenant
    bool        allow_stale       = false;
    std::string consistency;      // "eventual" | "strong"
};

} // namespace mercury
