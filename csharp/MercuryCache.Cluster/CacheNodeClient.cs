using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Mercury.Grpc;
using MercuryCache.Api.DTOs;

namespace MercuryCache.Cluster
{
    /// <summary>
    /// gRPC client wrapper for a single C++ cache node.
    /// All calls propagate trace IDs and respect timeout budgets.
    /// </summary>
    public class CacheNodeClient : IDisposable
    {
        private readonly GrpcChannel            _channel;
        private readonly CacheNodeService.CacheNodeServiceClient _stub;
        private readonly string                 _address;

        public string Address => _address;

        public CacheNodeClient(string address)
        {
            _address = address;
            _channel = GrpcChannel.ForAddress($"http://{address}",
                new GrpcChannelOptions
                {
                    MaxReceiveMessageSize = 64 * 1024 * 1024, // 64 MB
                    MaxSendMessageSize    = 64 * 1024 * 1024
                });
            _stub = new CacheNodeService.CacheNodeServiceClient(_channel);
        }

        // ── GET ─────────────────────────────────────────────────────────────
        public async Task<GetCacheResponse> GetAsync(
            string ns, string key, string consistency, bool allowStale,
            string traceId, CancellationToken ct = default)
        {
            var req = new CacheGetRequest
            {
                Namespace_   = ns,
                Key          = key,
                Consistency  = consistency,
                AllowStale   = allowStale,
                TraceId      = traceId,
                DeadlineMs   = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 5000
            };
            var resp = await _stub.GetAsync(req, cancellationToken: ct);

            return new GetCacheResponse
            {
                Key            = key,
                Found          = resp.Found,
                Value          = resp.Found ? resp.Value.ToStringUtf8() : null,
                Source         = resp.Source,
                TtlRemainingMs = resp.TtlRemainingMs,
                Version        = resp.Version,
                Stale          = resp.Stale,
                TraceId        = resp.TraceId
            };
        }

        // ── PUT ─────────────────────────────────────────────────────────────
        public async Task PutAsync(
            string ns, string key, string value,
            long ttlMs, long version, string writeMode, string traceId,
            CancellationToken ct = default)
        {
            var req = new CachePutRequest
            {
                Namespace_  = ns,
                Key         = key,
                Value       = Google.Protobuf.ByteString.CopyFromUtf8(value),
                TtlMs       = ttlMs,
                Version     = version,
                WriteMode   = writeMode,
                TraceId     = traceId
            };
            await _stub.PutAsync(req, cancellationToken: ct);
        }

        // ── DELETE ──────────────────────────────────────────────────────────
        public async Task DeleteAsync(
            string ns, string key, string traceId, CancellationToken ct = default)
        {
            var req = new CacheDeleteRequest
            {
                Namespace_ = ns,
                Key        = key,
                TraceId    = traceId
            };
            await _stub.DeleteAsync(req, cancellationToken: ct);
        }

        // ── REPLICATE ────────────────────────────────────────────────────────
        public async Task ReplicateAsync(
            string ns, string key, string value,
            long ttlMs, long version, string primaryNodeId, string traceId,
            CancellationToken ct = default)
        {
            var req = new ReplicateWriteRequest
            {
                Namespace_    = ns,
                Key           = key,
                Value         = Google.Protobuf.ByteString.CopyFromUtf8(value),
                TtlMs         = ttlMs,
                Version       = version,
                PrimaryNodeId = primaryNodeId,
                TraceId       = traceId
            };
            await _stub.ReplicateAsync(req, cancellationToken: ct);
        }

        // ── INVALIDATE ───────────────────────────────────────────────────────
        public async Task InvalidateAsync(
            string ns, string key, long minVersion, string reason,
            string traceId, CancellationToken ct = default)
        {
            var req = new InvalidateRequest
            {
                Namespace_  = ns,
                Key         = key,
                MinVersion  = minVersion,
                Reason      = reason,
                TraceId     = traceId
            };
            await _stub.InvalidateAsync(req, cancellationToken: ct);
        }

        // ── HEARTBEAT ────────────────────────────────────────────────────────
        public async Task<HeartbeatResponse> HeartbeatAsync(
            string traceId, CancellationToken ct = default)
        {
            var req = new HeartbeatRequest
            {
                NodeId    = _address,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
            return await _stub.HeartbeatAsync(req, cancellationToken: ct);
        }

        // ── WARM KEY ─────────────────────────────────────────────────────────
        public async Task WarmKeyAsync(
            string ns, string key, string traceId,
            CancellationToken ct = default)
        {
            var req = new WarmKeyRequest
            {
                Namespace_ = ns,
                Key        = key,
                TraceId    = traceId
            };
            await _stub.WarmKeyAsync(req, cancellationToken: ct);
        }

        public void Dispose() => _channel.Dispose();
    }

    /// <summary>
    /// Factory that caches gRPC channel instances per node address.
    /// </summary>
    public class CacheNodeClientFactory
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary
            <string, CacheNodeClient> _clients = new();

        public CacheNodeClient GetClient(string address)
            => _clients.GetOrAdd(address, a => new CacheNodeClient(a));
    }
}
