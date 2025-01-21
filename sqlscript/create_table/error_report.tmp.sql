CREATE TABLE IF NOT EXISTS error_report{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    is_drop Bool,
    trace_id String CODEC(ZSTD(1)),
    duration UInt64 CODEC(ZSTD(1)),
    end_time UInt64 CODEC(ZSTD(1)),
    drop_reason String CODEC(ZSTD(1)),
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    cause String CODEC(ZSTD(1)),
    cause_message String CODEC(ZSTD(1)),
    relation_trees String CODEC(ZSTD(1)),
    threshold_type String CODEC(ZSTD(1)),
    threshold_range String CODEC(ZSTD(1)),
    threshold_value Float64,
    threshold_multiple Float64,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp), trace_id)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1
