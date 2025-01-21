package redis

import "github.com/CloudDetail/apo-module/model/v1"

type ExpirableCache interface {
	Start()

	IsLocal() bool

	// Cacher
	StoreMetric(metric *model.OnOffMetricGroup, json string)
	GetMetricSize(traceId string) int
	GetMetrics(traceId string) []*model.OnOffMetricGroup

	StoreTrace(trace *model.Trace, json string)
	GetTraceSize(traceId string) int
	GetTraces(traceId string) []*model.Trace

	RecordTraceTime(traceId string, time int64)
	GetTraceTime(traceId string) int64

	// Keep Receiver TraceId index unique.
	IncrTraceIndex() int64
	GetTraceIndex() int64

	//Publisher
	NotifySampledTraceIds(normalTraceIds []string, slowTraceIds []string, errorTraceIds []string)

	// Subscriber
	SubscribeTraceIds(normalSubscriber Subscriber, slowSubscriber Subscriber, errorSubscriber Subscriber)

	// Stream + ConsumeGroup
	NotifyReportTraceId(traceId string)
	SubscribeReportTraceId(subscriber Subscriber)

	// Signal
	StoreSignal(nodeIp string, json string)
	GetAndCleanSignals(nodeIp string) []string

	StoreRelationTraceId(key string, traceId string)
	GetRelationTraceId(key string) string

	// Sampler
	GetSampleValue() int64
	InitSampleValue(sampleValue int64, expirePeriod int64)
	SetSampleValue(sampleValue int64, expirePeriod int64)

	LockAndCheckSampleTime() bool
}

type Subscriber interface {
	Consume(traceId string)
}

type TracesCallable interface {
	AddTask(traceId string, datas []string)
}
