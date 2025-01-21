package redis

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/CloudDetail/apo-module/model/v1"
)

type LocalCache struct {
	expireTime   int64
	traceMap     sync.Map // <traceId, ExpirableList>
	checkMissMap sync.Map // <traceId, ExpireData>
	signalMap    sync.Map
	relationMap  sync.Map
	sampleValue  *atomic.Int64
	sampleTime   *atomic.Int64

	mutex          sync.RWMutex
	reportTraceIds []string
	normalTraceIds []string
	slowTraceIds   []string
	errorTraceIds  []string
	stopChan       chan bool
}

func NewLocalCache(expireTime int64) *LocalCache {
	return &LocalCache{
		expireTime:     expireTime,
		sampleValue:    &atomic.Int64{},
		sampleTime:     &atomic.Int64{},
		reportTraceIds: make([]string, 0),
		slowTraceIds:   make([]string, 0),
		errorTraceIds:  make([]string, 0),
		stopChan:       make(chan bool),
	}
}

func (cache *LocalCache) Start() {
	go cache.checkExpire()
}

func (cache *LocalCache) Stop() {
	close(cache.stopChan)
}

func (cache *LocalCache) IsLocal() bool {
	return true
}

func (cache *LocalCache) StoreMetric(metric *model.OnOffMetricGroup, json string) {
	var expirableList *ExpirableList
	if listInterface, ok := cache.traceMap.Load(metric.TraceId); ok {
		expirableList = listInterface.(*ExpirableList)
	} else {
		expirableList = newExpirableList()
		cache.traceMap.Store(metric.TraceId, expirableList)
	}
	expirableList.addMetric(cache.expireTime, metric)
}

func (cache *LocalCache) GetMetricSize(traceId string) int {
	if listInterface, ok := cache.traceMap.Load(traceId); ok {
		expirableList := listInterface.(*ExpirableList)
		return len(expirableList.metrics)
	} else {
		return 0
	}
}

func (cache *LocalCache) GetMetrics(traceId string) []*model.OnOffMetricGroup {
	if listInterface, ok := cache.traceMap.Load(traceId); ok {
		expirableList := listInterface.(*ExpirableList)
		return expirableList.metrics
	} else {
		return nil
	}
}

func (cache *LocalCache) StoreTrace(trace *model.Trace, _json string) {
	var expirableList *ExpirableList
	if listInterface, ok := cache.traceMap.Load(trace.Labels.TraceId); ok {
		expirableList = listInterface.(*ExpirableList)
	} else {
		expirableList = newExpirableList()
		cache.traceMap.Store(trace.Labels.TraceId, expirableList)
	}
	expirableList.addTrace(cache.expireTime, trace)
}

func (cache *LocalCache) GetTraceSize(traceId string) int {
	if listInterface, ok := cache.traceMap.Load(traceId); ok {
		expirableList := listInterface.(*ExpirableList)
		return len(expirableList.traces)
	} else {
		return 0
	}
}

func (cache *LocalCache) GetTraces(traceId string) []*model.Trace {
	if listInterface, ok := cache.traceMap.Load(traceId); ok {
		expirableList := listInterface.(*ExpirableList)
		return expirableList.traces
	} else {
		return nil
	}
}

func (cache *LocalCache) RecordTraceTime(traceId string, data int64) {
	cache.checkMissMap.Store(traceId, newExpirableData(cache.expireTime, data))
}

func (cache *LocalCache) GetTraceTime(traceId string) int64 {
	if expirableData, ok := cache.checkMissMap.Load(traceId); ok {
		value := expirableData.(*ExpirableData[int64])
		return value.data
	}
	return 0
}

func (cache *LocalCache) GetTraceIndex() int64 {
	return time.Now().UnixNano()
}

func (cache *LocalCache) IncrTraceIndex() int64 {
	return time.Now().UnixNano()
}

func (cache *LocalCache) NotifyReportTraceId(traceId string) {
	cache.mutex.Lock()
	cache.reportTraceIds = append(cache.reportTraceIds, traceId)
	cache.mutex.Unlock()
}

func (cache *LocalCache) NotifySampledTraceIds(normalTraceIds []string, slowTraceIds []string, errorTraceIds []string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if len(normalTraceIds) > 0 {
		cache.normalTraceIds = append(cache.normalTraceIds, normalTraceIds...)
	}
	if len(slowTraceIds) > 0 {
		cache.slowTraceIds = append(cache.slowTraceIds, slowTraceIds...)
	}
	if len(errorTraceIds) > 0 {
		cache.errorTraceIds = append(cache.errorTraceIds, errorTraceIds...)
	}
}

func (cache *LocalCache) SubscribeReportTraceId(subscriber Subscriber) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			if len(cache.reportTraceIds) > 0 {
				cache.mutex.Lock()
				for _, traceId := range cache.reportTraceIds {
					subscriber.Consume(traceId)
				}
				cache.reportTraceIds = cache.reportTraceIds[0:0]
				cache.mutex.Unlock()
			}
		case <-cache.stopChan:
			timer.Stop()
			return
		}
	}
}

func (cache *LocalCache) SubscribeTraceIds(normalSubscriber Subscriber, slowSubscriber Subscriber, errorSubscriber Subscriber) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			normalCount := len(cache.normalTraceIds)
			if normalCount > 0 {
				cache.mutex.Lock()
				normalTraceIds := cache.normalTraceIds[0:normalCount]
				cache.normalTraceIds = cache.normalTraceIds[normalCount:]
				cache.mutex.Unlock()

				for _, traceId := range normalTraceIds {
					normalSubscriber.Consume(traceId)
				}
			}

			slowCount := len(cache.slowTraceIds)
			if slowCount > 0 {
				cache.mutex.Lock()
				slowTraceIds := cache.slowTraceIds[0:slowCount]
				cache.slowTraceIds = cache.slowTraceIds[slowCount:]
				cache.mutex.Unlock()

				for _, traceId := range slowTraceIds {
					slowSubscriber.Consume(traceId)
				}
			}

			errorCount := len(cache.errorTraceIds)
			if errorCount > 0 {
				cache.mutex.Lock()
				errorTraceIds := cache.errorTraceIds[0:errorCount]
				cache.errorTraceIds = cache.errorTraceIds[errorCount:]
				cache.mutex.Unlock()

				for _, traceId := range errorTraceIds {
					errorSubscriber.Consume(traceId)
				}
			}
		case <-cache.stopChan:
			timer.Stop()
			return
		}
	}
}

func (cache *LocalCache) checkExpire() {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-timer.C:
			checkTime := time.Now().Unix()
			cache.traceMap.Range(func(k, v interface{}) bool {
				sentTraces := v.(*ExpirableList)
				if sentTraces.expireTime < checkTime {
					cache.traceMap.Delete(k)
				}
				return true
			})
			cache.relationMap.Range(func(k, v interface{}) bool {
				relation := v.(*ExpirableData[string])
				if relation.expireTime < checkTime {
					cache.relationMap.Delete(k)
				}
				return true
			})

			cache.checkMissMap.Range(func(k, v interface{}) bool {
				value := v.(*ExpirableData[int64])
				if value.expireTime < checkTime {
					cache.checkMissMap.Delete(k)
				}
				return true
			})
		case <-cache.stopChan:
			timer.Stop()
			return
		}
	}
}

func (cache *LocalCache) StoreSignal(nodeIp string, json string) {
	var signal *SignalList
	if signalInterface, ok := cache.signalMap.Load(nodeIp); ok {
		signal = signalInterface.(*SignalList)
	} else {
		signal = newSignalList()
		cache.signalMap.Store(nodeIp, signal)
	}
	signal.addSignal(json)
}

func (cache *LocalCache) GetAndCleanSignals(nodeIp string) []string {
	if signalInterface, ok := cache.signalMap.Load(nodeIp); ok {
		signal := signalInterface.(*SignalList)
		return signal.getAndCleanSignals()
	}
	return nil
}

func (cache *LocalCache) StoreRelationTraceId(key string, traceId string) {
	if _, ok := cache.relationMap.Load(key); !ok {
		cache.relationMap.Store(key, newExpirableData(60, traceId))
	}
}

func (cache *LocalCache) GetRelationTraceId(key string) string {
	if dataInterface, ok := cache.relationMap.Load(key); ok {
		expirableData := dataInterface.(*ExpirableData[string])
		return expirableData.data
	}
	return ""
}

// SampleValue
func (cache *LocalCache) GetSampleValue() int64 {
	return cache.sampleValue.Load()
}

func (cache *LocalCache) InitSampleValue(sampleValue int64, expirePeriod int64) {
	cache.sampleValue.Store(sampleValue)
	cache.sampleTime.Store(time.Now().Unix() + expirePeriod)
}

func (cache *LocalCache) SetSampleValue(sampleValue int64, expirePeriod int64) {
	cache.sampleValue.Store(sampleValue)
	cache.sampleTime.Store(time.Now().Unix() + expirePeriod)
}

func (cache *LocalCache) LockAndCheckSampleTime() bool {
	now := time.Now().Unix()
	return now > cache.sampleTime.Load()
}

type ExpirableList struct {
	lock       sync.Mutex
	expireTime int64
	traces     []*model.Trace
	metrics    []*model.OnOffMetricGroup
}

func newExpirableList() *ExpirableList {
	return &ExpirableList{
		traces:  make([]*model.Trace, 0),
		metrics: make([]*model.OnOffMetricGroup, 0),
	}
}

func (list *ExpirableList) addTrace(expireTime int64, trace *model.Trace) {
	list.lock.Lock()
	defer list.lock.Unlock()
	list.expireTime = time.Now().Unix() + expireTime
	list.traces = append(list.traces, trace)
}

func (list *ExpirableList) addMetric(expireTime int64, metric *model.OnOffMetricGroup) {
	list.lock.Lock()
	defer list.lock.Unlock()
	list.expireTime = time.Now().Unix() + expireTime
	list.metrics = append(list.metrics, metric)
}

type ExpirableData[T any] struct {
	expireTime int64
	data       T
}

func newExpirableData[T any](expireTime int64, data T) *ExpirableData[T] {
	return &ExpirableData[T]{
		expireTime: time.Now().Unix() + expireTime,
		data:       data,
	}
}

type SignalList struct {
	mutex   sync.RWMutex
	signals []string
}

func newSignalList() *SignalList {
	return &SignalList{
		mutex:   sync.RWMutex{},
		signals: make([]string, 0),
	}
}

func (list *SignalList) addSignal(json string) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	list.signals = append(list.signals, json)
}

func (list *SignalList) getAndCleanSignals() []string {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var signals []string
	size := len(list.signals)
	if size > 0 {
		// Copy
		signals = list.signals[0:size]
		// Clean
		list.signals = list.signals[size:]
	}
	return signals
}
