package clickhouse

import (
	"bytes"
	"testing"
	"text/template"
)

func TestTemplate(t *testing.T) {
	args := distributedTableArgs{
		Cluster:  "apocluster",
		Database: "apo",
	}
	migrateTemplate, err := template.ParseFiles("../../sqlscript/migrate.tmpl.sql")
	if err != nil {
		t.Failed()
		return
	}
	var rendered bytes.Buffer
	if err := migrateTemplate.Execute(&rendered, args); err != nil {
		t.Failed()
		return
	}
	sqlStatement := rendered.String()
	t.Logf("%s", sqlStatement)
}
