CREATE TABLE IF NOT EXISTS service_relationship{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    entry_service LowCardinality(String) CODEC(ZSTD(1)),
    entry_url String CODEC(ZSTD(1)),
    miss_top Bool,
    trace_id String CODEC(ZSTD(1)),
    parent_service String CODEC(ZSTD(1)),
    parent_url String CODEC(ZSTD(1)),
    service String CODEC(ZSTD(1)),
    url String CODEC(ZSTD(1)),
    path String CODEC(ZSTD(1)),
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    flags Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp), trace_id)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1