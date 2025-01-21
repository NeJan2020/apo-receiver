CREATE TABLE IF NOT EXISTS span_trace{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    data_version LowCardinality(String) CODEC(ZSTD(1)),
    pid UInt32,
    tid UInt32,
    report_type UInt32,
    threshold_type String CODEC(ZSTD(1)),
    threshold_range String CODEC(ZSTD(1)),
    threshold_value Float64,
    threshold_multiple Float64,
    trace_id String CODEC(ZSTD(1)),
    apm_span_id String CODEC(ZSTD(1)),
    flags Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    start_time UInt64 CODEC(ZSTD(1)),
    duration UInt64 CODEC(ZSTD(1)),
    end_time UInt64 CODEC(ZSTD(1)),
    offset_ts Int64,
    metrics Map(LowCardinality(String), UInt64) CODEC(ZSTD(1)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id apm_span_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp), trace_id)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1