CREATE TABLE IF NOT EXISTS onoff_metric{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    pid UInt32,
    tid UInt32,
    container_id String CODEC(ZSTD(1)),
    trace_id String CODEC(ZSTD(1)),
    apm_span_id String CODEC(ZSTD(1)),
    metrics String CODEC(ZSTD(1)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id apm_span_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp), trace_id)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1