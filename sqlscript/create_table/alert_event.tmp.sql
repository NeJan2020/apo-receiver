CREATE TABLE IF NOT EXISTS alert_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    source String,
    group String,
    id UUID,
    create_time DATETIME64(3),
    update_time DATETIME64(3),
    end_time DATETIME64(3),
    received_time DATETIME64(3),
    severity Enum8('unknown' = 0, 'info' = 1, 'warning' = 2, 'error' = 3, 'critical' = 4),
    name String,
    detail String,
    tags Map(String, String),
    status Enum8('resolved' = 0, 'firing' = 1)
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(received_time)
    ORDER BY (source, received_time)
    TTL toDateTime(received_time) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1