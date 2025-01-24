-- 1.3.0
ALTER TABLE alert_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}} ADD COLUMN `alert_id` String CODEC(ZSTD(1));
ALTER TABLE alert_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}} ADD COLUMN `raw_tags` Map(LowCardinality(String), String) CODEC(ZSTD(1));
ALTER TABLE alert_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}} ADD COLUMN `source_id` LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE alert_event{{if .Cluster}}_local ON CLUSTER {{.Cluster}}{{end}} MODIFY COLUMN `tags` Map(LowCardinality(String), String) CODEC(ZSTD(1));

{{if .Cluster}}
drop table alert_event on CLUSTER {{.Cluster}};
CREATE TABLE IF NOT EXISTS alert_event
    ON CLUSTER {{.Cluster}} AS {{.Database}}.alert_event_local
ENGINE = Distributed('{{.Cluster}}', '{{.Database}}', 'alert_event_local', cityHash64(trace_id));
{{end}}