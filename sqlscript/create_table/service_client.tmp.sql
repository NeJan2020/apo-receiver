CREATE TABLE IF NOT EXISTS service_client{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    entry_service LowCardinality(String) CODEC(ZSTD(1)),
    entry_url String CODEC(ZSTD(1)),
    miss_top Bool,
    trace_id String CODEC(ZSTD(1)),
    client_span_id String CODEC(ZSTD(1)),
    client_service String CODEC(ZSTD(1)),
    client_url String CODEC(ZSTD(1)),
    next_span_id String CODEC(ZSTD(1)),
    client_time UInt64 CODEC(ZSTD(1)),
    client_duration UInt64 CODEC(ZSTD(1)),
    client_error Bool,
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1))
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp))
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1