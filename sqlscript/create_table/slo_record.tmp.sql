CREATE TABLE IF NOT EXISTS slo_record{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
    entryUri String,
    entryService String,
    alias String,
    startTime Int64,
    endTime Int64,
    requestCount Int64,
    status String,
    SLOs Nested (
        type String,
        multiple Float64,
        expectedValue Float64,
        source String,
        currentValue Float64,
        status String
    ),
    slowRootCauseCount Nested (
        key String,
        value UInt32
    ),
    errorRootCauseCount Nested (
        key String,
        value UInt32
    ),
    step String,
    indexTimestamp Int64
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    ORDER BY indexTimestamp
    TTL toDateTime(indexTimestamp / 1000) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity = 8192