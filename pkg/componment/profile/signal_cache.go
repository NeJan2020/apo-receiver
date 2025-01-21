package profile

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/CloudDetail/apo-module/model/v1"
	profile_model "github.com/CloudDetail/apo-receiver/pkg/componment/profile/model"
	"github.com/CloudDetail/apo-receiver/pkg/global"
	grpc_model "github.com/CloudDetail/apo-receiver/pkg/model"
)

const (
	CameraReportMetric = "camera_report_metric"
)

type SingalsCache struct {
	cache sync.Map // <nodeIp, SignalCache>
}

func newSignalsCache() *SingalsCache {
	return &SingalsCache{}
}

func (signals *SingalsCache) AddSignal(entryService string, entryUrl string, trace *model.Trace, needProfile bool) {
	var signal *SignalCache
	if signalInterface, ok := signals.cache.Load(trace.Labels.NodeIp); ok {
		signal = signalInterface.(*SignalCache)
	} else {
		signal = newSignalCache()
		signals.cache.Store(trace.Labels.NodeIp, signal)
	}
	signal.addSignal(entryService, entryUrl, trace, needProfile)
}

func (signals *SingalsCache) QuerySilentSwitches(nodeIp string) ([]string, []string) {
	if signalInterface, ok := signals.cache.Load(nodeIp); ok {
		signal := signalInterface.(*SignalCache)
		return signal.querySilentSwitches()
	}
	return nil, nil
}

func (signals *SingalsCache) CollectMetrics() {
	timer := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-timer.C:
			signals.cache.Range(func(k, v interface{}) bool {
				countMetrics := v.(*SignalCache).collectCountMetrics()
				if len(countMetrics) > 0 {
					log.Printf("[Write Slow Report Metics] Count: %d", len(countMetrics))
					for _, countMetric := range countMetrics {
						global.CLICK_HOUSE.StoreReportMetric(countMetric)
					}
				}
				return true
			})
		}
	}
}

type SignalCache struct {
	mutex   sync.RWMutex
	metrics sync.Map // <slowReportTuple, *slowReportMetric>
}

func newSignalCache() *SignalCache {
	return &SignalCache{
		mutex: sync.RWMutex{},
	}
}

func (cache *SignalCache) addSignal(entryService string, entryUrl string, trace *model.Trace, needProfile bool) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	tuple := slowReportTuple{
		entryService:   entryService,
		entryUrl:       entryUrl,
		mutatedService: trace.GetInstanceId(),
		mutatedPid:     trace.Labels.Pid,
		mutatedUrl:     trace.Labels.Url,
	}

	var metric *slowReportMetric
	if metricInterface, ok := cache.metrics.Load(tuple); ok {
		metric = metricInterface.(*slowReportMetric)
	} else {
		metric = newSlowReportMetric()
		cache.metrics.Store(tuple, metric)
	}
	metric.addMetric(trace.Labels.IsProfiled)

	if needProfile {
		signalJson, _ := json.Marshal(&grpc_model.ProfileSignal{
			Pid:       trace.Labels.Pid,
			Tid:       trace.Labels.Tid,
			StartTime: trace.Labels.StartTime,
			EndTime:   trace.Labels.EndTime,
		})
		global.CACHE.StoreSignal(trace.Labels.NodeIp, string(signalJson))
	}
}

func (cache *SignalCache) querySilentSwitches() ([]string, []string) {
	toCloses := make(map[string]bool, 0)
	toRecovers := make(map[string]bool, 0)
	ignores := make(map[string]bool, 0)

	cache.metrics.Range(func(k, v any) bool {
		key := k.(slowReportTuple)
		value := v.(*slowReportMetric)

		pidUrl := key.getPidUrl()
		toClose, toRecover, silent := value.checkStatus()
		if toClose {
			toCloses[pidUrl] = true
		}
		if toRecover {
			toRecovers[pidUrl] = true
		}
		if silent {
			ignores[pidUrl] = true
		}
		return true
	})

	closePidUrls := make([]string, 0)
	recoverPidUrls := make([]string, 0)
	if len(toCloses) > 0 {
		for key := range toCloses {
			closePidUrls = append(closePidUrls, key)
		}
	}
	if len(toRecovers) > 0 {
		for key := range ignores {
			delete(toRecovers, key)
		}
		for key := range toRecovers {
			recoverPidUrls = append(recoverPidUrls, key)
		}
	}
	return closePidUrls, recoverPidUrls
}

func (cache *SignalCache) collectCountMetrics() []*profile_model.SlowReportCountMetric {
	result := make([]*profile_model.SlowReportCountMetric, 0)
	now := time.Now().UnixMilli() * 1e6
	cache.metrics.Range(func(k, v any) bool {
		key := k.(slowReportTuple)
		value := v.(*slowReportMetric)

		if value.total > 0 {
			result = append(result, &profile_model.SlowReportCountMetric{
				Name:           CameraReportMetric,
				Timestamp:      now,
				EntryService:   key.entryService,
				EntryUrl:       key.entryUrl,
				MutatedService: key.mutatedService,
				MutatedUrl:     key.mutatedUrl,
				Total:          value.total,
				Success:        value.success,
			})
		}
		cache.metrics.Delete(k)
		return true
	})

	return result
}

type slowReportTuple struct {
	entryService   string
	entryUrl       string
	mutatedService string
	mutatedPid     uint32
	mutatedUrl     string
}

func (tuple *slowReportTuple) getPidUrl() string {
	return fmt.Sprintf("%d-%s", tuple.mutatedPid, tuple.mutatedUrl)
}

type SilentStatus int

const (
	Init SilentStatus = iota
	Closing
	Finished
)

type slowReportMetric struct {
	total   int
	success int
	status  SilentStatus
}

func newSlowReportMetric() *slowReportMetric {
	return &slowReportMetric{
		total:   0,
		success: 0,
		status:  Init,
	}
}

func (metric *slowReportMetric) addMetric(success bool) {
	metric.total += 1
	if success {
		metric.success += 1
	}
}

func (metric *slowReportMetric) checkStatus() (toClose bool, toRecover bool, silent bool) {
	if metric.status == Init {
		if metric.total > 0 {
			if metric.success == 0 {
				metric.status = Closing
				toClose = true
			} else {
				metric.status = Finished
			}
		}
	} else if metric.status == Closing {
		if metric.success > 0 {
			toRecover = true
			metric.status = Finished
		}
	}
	silent = metric.status == Closing
	return toClose, toRecover, silent
}
