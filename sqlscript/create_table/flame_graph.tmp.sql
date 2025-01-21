CREATE TABLE IF NOT EXISTS flame_graph{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    start_time DateTime64(9) CODEC(Delta, ZSTD(1)),
    end_time DateTime64(9) CODEC(Delta, ZSTD(1)),
    pid UInt32,
    tid UInt32,
    sample_type LowCardinality(String) CODEC(ZSTD(1)),
    sample_rate UInt32,
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    flamebearer String CODEC(ZSTD(1)),
    INDEX idx_pid pid TYPE minmax GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(start_time)
    ORDER BY (pid, toUnixTimestamp(start_time))
    TTL toDateTime(start_time) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1