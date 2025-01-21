CREATE TABLE IF NOT EXISTS detect_list{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    `timestamp` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `start_time` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `end_time` DateTime64(9) CODEC(Delta, ZSTD(1)),
    `step` Int64,
    `name` String,
    `mutation_check_pql` String,
    `for_duration` String,
    `synchronize_to_alert_rules` Bool DEFAULT 0,
    `group` String
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY toUnixTimestamp(timestamp)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1