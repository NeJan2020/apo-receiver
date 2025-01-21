CREATE TABLE IF NOT EXISTS error_propagation{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}}
(
	timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	entry_service LowCardinality(String) CODEC(ZSTD(1)),
	entry_url String CODEC(ZSTD(1)),
	entry_span_id String CODEC(ZSTD(1)),
	trace_id String CODEC(ZSTD(1)),
	nodes Nested (
		service String,
		instance String,
		url String,
		is_traced Bool,
		is_error Bool,
    	error_types Array(String),
		error_msgs Array(String),
		depth UInt32,
		path String
	) CODEC(ZSTD(1)),
	INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    PARTITION BY toDate(timestamp)
    ORDER BY (toUnixTimestamp(timestamp), trace_id)
    TTL toDateTime(timestamp) + toIntervalDay({{.TTLDay}})
    SETTINGS index_granularity=8192, ttl_only_drop_parts = 1