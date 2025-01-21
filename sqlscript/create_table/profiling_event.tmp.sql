CREATE TABLE IF NOT EXISTS profiling_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    data_version LowCardinality(String) CODEC(ZSTD(1)),
    pid UInt32,
    tid UInt32,
    startTime UInt64,
    endTime UInt64,
    cpuEvents String CODEC(ZSTD(1)),
    innerCalls String CODEC(ZSTD(1)),
    javaFutexEvents String CODEC(ZSTD(1)),
    spans String CODEC(ZSTD(1)),
    transactionIds String CODEC(ZSTD(1)),
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    offset_ts Int64,
    INDEX idx_pid pid TYPE minmax GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (pid, toUnixTimestamp(timestamp))
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1