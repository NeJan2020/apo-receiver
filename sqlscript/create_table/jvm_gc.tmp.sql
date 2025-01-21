CREATE TABLE IF NOT EXISTS jvm_gc{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    pid String CODEC(ZSTD(1)),
    labels Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ygc Int64,
    fgc Int64,
    last_ygc Int64,
    last_fgc Int64,
    ygc_last_entry_time Int64,
    fgc_last_entry_time Int64,
    ygc_span Int64,
    fgc_span Int64,
    INDEX idx_pid pid TYPE minmax GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (pid, toUnixTimestamp(timestamp))
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1
