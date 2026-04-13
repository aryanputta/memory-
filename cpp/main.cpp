// MercuryCache++ — Cache Node Server entry point
//
// Usage:
//   cache_node_server [address] [node_id] [max_cache_mb]
//   e.g.: cache_node_server 0.0.0.0:50051 node-0 512

#include "cache_core/CacheStore.h"
#include "eviction/LruEvictionPolicy.h"
#include "eviction/LfuEvictionPolicy.h"
#include "eviction/TinyLfuAdmissionPolicy.h"
#include "hotkeys/HotKeyTracker.h"
#include "grpc/CacheNodeServer.h"
#include <iostream>
#include <memory>
#include <cstdlib>
#include <string>

int main(int argc, char* argv[]) {
    std::string address  = "0.0.0.0:50051";
    std::string node_id  = "node-0";
    size_t      max_mb   = 256;

    if (argc > 1) address = argv[1];
    if (argc > 2) node_id = argv[2];
    if (argc > 3) max_mb  = static_cast<size_t>(std::atol(argv[3]));

    size_t max_bytes = max_mb * 1024ULL * 1024ULL;

    std::cout << "=== MercuryCache++ Node ===\n"
              << "  node_id  : " << node_id  << "\n"
              << "  address  : " << address  << "\n"
              << "  max cache: " << max_mb   << " MB\n";

    // Build eviction policy: LRU backbone + TinyLFU admission
    auto lru      = std::make_unique<mercury::LruEvictionPolicy>();
    auto tinylfu  = std::make_unique<mercury::TinyLfuAdmissionPolicy>(/*sample_size=*/100000);

    // Raw pointer kept for the store (store takes ownership of the TinyLFU)
    mercury::TinyLfuAdmissionPolicy* tinylfu_ptr = tinylfu.get();
    (void)tinylfu_ptr; // used via unique_ptr below

    mercury::CacheStore store(max_bytes, std::move(lru), std::move(tinylfu));
    mercury::HotKeyTracker hot_keys(/*hot_threshold_qps=*/500,
                                    /*window_ms=*/60000,
                                    /*top_k=*/20);

    mercury::RunCacheNodeServer(address, store, hot_keys, node_id);
    return 0;
}
