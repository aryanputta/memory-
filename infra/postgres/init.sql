-- MercuryCache++ — Backing Store Schema
-- This is the source-of-truth database that the cache layer sits in front of.

CREATE TABLE IF NOT EXISTS cache_source_of_truth (
    namespace   VARCHAR(128)  NOT NULL,
    key         VARCHAR(512)  NOT NULL,
    payload     TEXT          NOT NULL DEFAULT '',
    version     BIGINT        NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, key)
);

-- Index for namespace scans (used by namespace flush operations)
CREATE INDEX IF NOT EXISTS idx_cst_namespace ON cache_source_of_truth (namespace);
CREATE INDEX IF NOT EXISTS idx_cst_updated   ON cache_source_of_truth (updated_at);

-- Seed some catalog data for benchmarking warm-up
INSERT INTO cache_source_of_truth (namespace, key, payload, version)
SELECT
    'catalog',
    'item:' || i,
    '{"id":' || i || ',"name":"Product ' || i || '","price":' || (random() * 999 + 1)::int || '}',
    1
FROM generate_series(1, 10000) AS i
ON CONFLICT DO NOTHING;

INSERT INTO cache_source_of_truth (namespace, key, payload, version)
SELECT
    'pricing',
    'sku:' || i,
    '{"sku":' || i || ',"price":' || (random() * 499 + 0.99)::numeric(8,2) || ',"currency":"USD"}',
    1
FROM generate_series(1, 5000) AS i
ON CONFLICT DO NOTHING;

COMMENT ON TABLE cache_source_of_truth IS
    'Source-of-truth store for MercuryCache++. '
    'The distributed cache is a read-acceleration layer in front of this table, '
    'mirroring the pattern of Amazon DynamoDB Accelerator (DAX) in front of DynamoDB.';
