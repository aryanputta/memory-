using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace MercuryCache.Core
{
    /// <summary>
    /// Thin adapter over the PostgreSQL backing store (source of truth).
    /// Schema: cache_source_of_truth(namespace, key, payload, version, updated_at)
    /// </summary>
    public class BackingStoreValue
    {
        public string Value   { get; init; } = "";
        public long   Version { get; init; }
        public DateTime UpdatedAt { get; init; }
    }

    public class BackingStoreAdapter
    {
        private readonly string _connectionString;

        public BackingStoreAdapter(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<BackingStoreValue?> ReadAsync(
            string ns, string key, CancellationToken ct = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                "SELECT payload, version, updated_at " +
                "FROM cache_source_of_truth " +
                "WHERE namespace = @ns AND key = @key " +
                "LIMIT 1", conn);

            cmd.Parameters.AddWithValue("ns",  ns);
            cmd.Parameters.AddWithValue("key", key);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return null;

            return new BackingStoreValue
            {
                Value     = reader.GetString(0),
                Version   = reader.GetInt64(1),
                UpdatedAt = reader.GetDateTime(2)
            };
        }

        public async Task WriteAsync(
            string ns, string key, string payload, long version,
            CancellationToken ct = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                "INSERT INTO cache_source_of_truth " +
                "  (namespace, key, payload, version, updated_at) " +
                "VALUES (@ns, @key, @payload, @version, NOW()) " +
                "ON CONFLICT (namespace, key) DO UPDATE " +
                "  SET payload = EXCLUDED.payload, " +
                "      version = EXCLUDED.version, " +
                "      updated_at = NOW() " +
                "  WHERE cache_source_of_truth.version <= EXCLUDED.version", conn);

            cmd.Parameters.AddWithValue("ns",      ns);
            cmd.Parameters.AddWithValue("key",     key);
            cmd.Parameters.AddWithValue("payload", payload);
            cmd.Parameters.AddWithValue("version", version);

            await cmd.ExecuteNonQueryAsync(ct);
        }

        public async Task DeleteAsync(
            string ns, string key, CancellationToken ct = default)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(
                "DELETE FROM cache_source_of_truth " +
                "WHERE namespace = @ns AND key = @key", conn);

            cmd.Parameters.AddWithValue("ns",  ns);
            cmd.Parameters.AddWithValue("key", key);

            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
