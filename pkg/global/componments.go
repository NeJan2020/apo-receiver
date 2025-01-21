package global

import (
	"github.com/CloudDetail/apo-receiver/pkg/clickhouse"
	"github.com/CloudDetail/apo-receiver/pkg/componment/redis"

	"github.com/CloudDetail/apo-module/apm/client/v1/api"
)

var (
	CLICK_HOUSE  *clickhouse.ClickHouseClient
	TRACE_CLIENT api.ApmTraceAPI
	CACHE        redis.ExpirableCache
	PROM_RANGE   string
)
