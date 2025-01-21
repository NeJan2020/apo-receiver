package onoffmetric

import (
	"testing"

	slomodel "github.com/CloudDetail/apo-module/slo/api/v1/model"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

func TestCalcMutatedType(t *testing.T) {
	prometheusClient, _ := api.NewClient(api.Config{
		Address: "http://localhost/test",
	})
	prometheusV1Api := v1.NewAPI(prometheusClient)
	CacheInstance = NewMetricCache(prometheusV1Api)

	key := MetricKey{
		ServiceName: "",
		ContentKey:  "/url",
	}
	key2 := MetricKey{
		ServiceName: "svc",
		ContentKey:  "/url",
	}
	CacheInstance.YesterdayMetricMap = map[MetricKey]*MetricDatas{
		key: {
			P90Values: [8]uint64{100, 100, 100, 100, 100, 100, 100, 100},
		},
	}

	mutatedCpuType, baseValue, thresholdRange := CalcMutatedType(slomodel.SLO_LATENCY_P90_TYPE, key, "120,130,150,0,100,99,80,0")
	checkStringEqual(t, "Mutated CpuType", "net", mutatedCpuType.String())
	checkStringEqual(t, "Base P90", "100,100,100,100,100,100,100,100", baseValue)
	checkStringEqual(t, "Threshold Range", "24h", thresholdRange)

	noMutatedCpuType, baseValue, _ := CalcMutatedType(slomodel.SLO_LATENCY_P90_TYPE, key2, "90,80,70,60,50,40,30,0")
	checkStringEqual(t, "Mutated CpuType", "unknown", noMutatedCpuType.String())
	checkStringEqual(t, "Base P90", "100,100,100,100,100,100,100,100", baseValue)

	key3 := MetricKey{
		ServiceName: "svc",
		ContentKey:  "/url2",
	}
	unknownUrlCpuType, baseValue, thresholdRange := CalcMutatedType(slomodel.SLO_LATENCY_P90_TYPE, key3, "120,170,150,0,100,99,80,0")
	checkStringEqual(t, "Mutated CpuType", "file", unknownUrlCpuType.String())
	checkStringEqual(t, "Base P90", "", baseValue)
	checkStringEqual(t, "Threshold Range", "unknown", thresholdRange)
}

func checkStringEqual(t *testing.T, key string, expect string, got string) {
	if expect != got {
		t.Errorf("[Check %s] want=%s, got=%s", key, expect, got)
	}
}
