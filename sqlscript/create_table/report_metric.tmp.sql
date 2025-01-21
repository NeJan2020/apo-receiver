CREATE TABLE IF NOT EXISTS report_metric{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    entry_service LowCardinality(String) CODEC(ZSTD(1)),
    entry_url String CODEC(ZSTD(1)),
    mutated_service LowCardinality(String) CODEC(ZSTD(1)),
    mutated_url String CODEC(ZSTD(1)),
    total UInt32 CODEC(ZSTD(1)),
    success UInt32 CODEC(ZSTD(1)),
    INDEX idx_entry_service entry_service TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (entry_service, toUnixTimestamp(timestamp))
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1