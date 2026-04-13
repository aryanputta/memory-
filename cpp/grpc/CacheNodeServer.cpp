// CacheNodeServer.cpp
// gRPC service implementation that wires the cache_node.proto service to the
// CacheStore, HotKeyTracker, and MetricsRegistry.
//
// Build dependencies:
//   - grpc++ (grpc/grpc.h)
//   - protobuf (google/protobuf/...)
//   - generated cache_node.grpc.pb.h / cache_node.pb.h  (from cache_node.proto)
//
// In production, compile with:
//   protoc --grpc_out=. --cpp_out=. cache_node.proto \
//     --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`

#include "CacheNodeServer.h"
#include "../cache_core/CacheStore.h"
#include "../cache_core/RequestContext.h"
#include "../hotkeys/HotKeyTracker.h"
#include "../metrics/MetricsRegistry.h"
#include <chrono>
#include <thread>
#include <iostream>

namespace mercury {

// ---------------------------------------------------------------------------
// Helper: wall-clock ms
// ---------------------------------------------------------------------------
static int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// ---------------------------------------------------------------------------
// CacheNodeServiceImpl
// ---------------------------------------------------------------------------
class CacheNodeServiceImpl final : public mercury::CacheNodeService::Service {
public:
    CacheNodeServiceImpl(CacheStore& store, HotKeyTracker& hot_keys,
                         const std::string& node_id)
        : store_(store), hot_keys_(hot_keys), node_id_(node_id)
    {}

    // ---- Get ---------------------------------------------------------------
    grpc::Status Get(grpc::ServerContext* /*ctx*/,
                     const CacheGetRequest* req,
                     CacheGetResponse* resp) override
    {
        auto start_us = NowUs();
        CacheKey key{req->namespace_(), req->key()};
        int64_t now = NowMs();

        RequestContext rctx;
        rctx.trace_id    = req->trace_id();
        rctx.now_ms      = now;
        rctx.allow_stale = req->allow_stale();
        rctx.consistency = req->consistency();
        rctx.hot_key_suspected = hot_keys_.IsHot(key);

        hot_keys_.RecordAccess(key);
        auto value_opt = store_.Get(key, now);

        if (value_opt.has_value()) {
            resp->set_found(true);
            resp->set_value(value_opt->payload);
            resp->set_source(value_opt->is_stale ? "stale_cache" : "cache");
            resp->set_ttl_remaining_ms(value_opt->TtlRemainingMs(now));
            resp->set_version(value_opt->version);
            resp->set_stale(value_opt->is_stale);
            MetricsRegistry::Global().GetCounter("cache_hits").Inc();
        } else {
            resp->set_found(false);
            resp->set_source("miss");
            MetricsRegistry::Global().GetCounter("cache_misses").Inc();
        }
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());

        RecordLatency("get_latency", start_us);
        return grpc::Status::OK;
    }

    // ---- Put ---------------------------------------------------------------
    grpc::Status Put(grpc::ServerContext* /*ctx*/,
                     const CachePutRequest* req,
                     CachePutResponse* resp) override
    {
        auto start_us = NowUs();
        CacheKey key{req->namespace_(), req->key()};

        RequestContext rctx;
        rctx.trace_id = req->trace_id();
        rctx.now_ms   = NowMs();

        CacheValue val(req->value(), req->version(), rctx.now_ms, req->ttl_ms());
        auto result = store_.Put(key, std::move(val), rctx);

        switch (result.status) {
            case PutResultStatus::Admitted: resp->set_status("admitted"); break;
            case PutResultStatus::Updated:  resp->set_status("updated");  break;
            case PutResultStatus::Rejected: resp->set_status("rejected"); break;
        }
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());

        MetricsRegistry::Global().GetCounter("cache_puts").Inc();
        RecordLatency("put_latency", start_us);
        return grpc::Status::OK;
    }

    // ---- Delete ------------------------------------------------------------
    grpc::Status Delete(grpc::ServerContext* /*ctx*/,
                        const CacheDeleteRequest* req,
                        CacheDeleteResponse* resp) override
    {
        CacheKey key{req->namespace_(), req->key()};
        bool deleted = store_.Delete(key);
        resp->set_deleted(deleted);
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());
        MetricsRegistry::Global().GetCounter("cache_deletes").Inc();
        return grpc::Status::OK;
    }

    // ---- BatchGet ----------------------------------------------------------
    grpc::Status BatchGet(grpc::ServerContext* /*ctx*/,
                          const CacheBatchGetRequest* req,
                          CacheBatchGetResponse* resp) override
    {
        auto start_us = NowUs();
        int64_t now = NowMs();
        std::vector<CacheKey> keys;
        for (const auto& k : req->keys())
            keys.emplace_back(req->namespace_(), k);

        auto results = store_.BatchGet(keys, now);
        for (const auto& [ck, val_opt] : results) {
            auto* item = resp->add_items();
            item->set_key(ck.key);
            if (val_opt.has_value()) {
                item->set_found(true);
                item->set_value(val_opt->payload);
                item->set_version(val_opt->version);
                item->set_stale(val_opt->is_stale);
                item->set_source(val_opt->is_stale ? "stale_cache" : "cache");
            } else {
                item->set_found(false);
            }
        }
        resp->set_trace_id(req->trace_id());
        RecordLatency("batch_get_latency", start_us);
        return grpc::Status::OK;
    }

    // ---- Heartbeat ---------------------------------------------------------
    grpc::Status Heartbeat(grpc::ServerContext* /*ctx*/,
                           const HeartbeatRequest* /*req*/,
                           HeartbeatResponse* resp) override
    {
        auto stats = store_.SnapshotStats();
        resp->set_node_id(node_id_);
        resp->set_healthy(true);
        resp->set_item_count(static_cast<int64_t>(stats.item_count));
        resp->set_bytes_used(static_cast<int64_t>(stats.bytes_used));
        return grpc::Status::OK;
    }

    // ---- Replicate ---------------------------------------------------------
    grpc::Status Replicate(grpc::ServerContext* /*ctx*/,
                           const ReplicateWriteRequest* req,
                           ReplicateWriteResponse* resp) override
    {
        CacheKey key{req->namespace_(), req->key()};
        RequestContext rctx;
        rctx.now_ms   = NowMs();
        rctx.trace_id = req->trace_id();

        CacheValue val(req->value(), req->version(), rctx.now_ms, req->ttl_ms());
        store_.Put(key, std::move(val), rctx);

        resp->set_status("ok");
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());
        MetricsRegistry::Global().GetCounter("replica_writes").Inc();
        return grpc::Status::OK;
    }

    // ---- Invalidate --------------------------------------------------------
    grpc::Status Invalidate(grpc::ServerContext* /*ctx*/,
                            const InvalidateRequest* req,
                            InvalidateResponse* resp) override
    {
        CacheKey key{req->namespace_(), req->key()};
        bool deleted = store_.Delete(key);
        resp->set_invalidated(deleted);
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());
        MetricsRegistry::Global().GetCounter("invalidations").Inc();
        return grpc::Status::OK;
    }

    // ---- WarmKey -----------------------------------------------------------
    grpc::Status WarmKey(grpc::ServerContext* /*ctx*/,
                         const WarmKeyRequest* req,
                         WarmKeyResponse* resp) override
    {
        CacheKey key{req->namespace_(), req->key()};
        RequestContext rctx;
        rctx.now_ms   = NowMs();
        rctx.trace_id = req->trace_id();

        CacheValue val(req->value(), req->version(), rctx.now_ms, req->ttl_ms());
        auto result = store_.Put(key, std::move(val), rctx);
        resp->set_warmed(result.status != PutResultStatus::Rejected);
        resp->set_node_id(node_id_);
        resp->set_trace_id(req->trace_id());
        return grpc::Status::OK;
    }

    // ---- ExportMetrics -----------------------------------------------------
    grpc::Status ExportMetrics(grpc::ServerContext* /*ctx*/,
                               const MetricsRequest* /*req*/,
                               MetricsResponse* resp) override
    {
        resp->set_prometheus_text(MetricsRegistry::Global().PrometheusExport());
        return grpc::Status::OK;
    }

private:
    static int64_t NowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    void RecordLatency(const std::string& name, int64_t start_us) {
        int64_t elapsed = NowUs() - start_us;
        MetricsRegistry::Global().GetHistogram(name).Record(
            static_cast<double>(elapsed));
    }

    CacheStore&    store_;
    HotKeyTracker& hot_keys_;
    std::string    node_id_;
};

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------
void RunCacheNodeServer(const std::string& address,
                        CacheStore& store,
                        HotKeyTracker& hot_keys,
                        const std::string& node_id)
{
    CacheNodeServiceImpl service(store, hot_keys, node_id);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024); // 64 MB

    auto server = builder.BuildAndStart();
    std::cout << "[CacheNode] " << node_id << " listening on " << address << "\n";

    // Background cleanup thread: remove expired entries every 5 seconds
    std::thread cleanup_thread([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            size_t removed = store.CleanupExpired(NowMs());
            if (removed > 0) {
                std::cout << "[CacheNode] Cleaned up " << removed
                          << " expired entries\n";
                MetricsRegistry::Global().GetCounter("ttl_expirations").Inc(removed);
            }
        }
    });
    cleanup_thread.detach();

    server->Wait();
}

} // namespace mercury
