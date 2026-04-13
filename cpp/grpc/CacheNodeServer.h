#pragma once
#include "../cache_core/CacheStore.h"
#include "../hotkeys/HotKeyTracker.h"
#include <string>

// Forward declare generated gRPC types to avoid including heavy protobuf
// headers here. Include cache_node.grpc.pb.h in .cpp files only.
namespace grpc { class Server; }

namespace mercury {

// Start the gRPC server on the given address (e.g., "0.0.0.0:50051").
// Blocks until the server is shut down.
void RunCacheNodeServer(const std::string& address,
                        CacheStore& store,
                        HotKeyTracker& hot_keys,
                        const std::string& node_id);

} // namespace mercury
