package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/CloudDetail/apo-module/model/v1"
)

const (
	REDIS_KEY_TRACE  = "kd-span-trace-%s"
	REDIS_KEY_METRIC = "kd-onoff-metric-%s"
	REDIS_KEY_SIGNAL = "kd-signal-%s"

	REDIS_KEY_LAST_TRACE = "kd-last-trace-%s"

	REDIS_KEY_SENT_RELATION = "kd-sent-relation-%s"

	REDIS_KEY_TRACE_INDEX = "kd-traceIndex"

	REDIS_CHANNEL_NORMAL = "kd-normalChannel"
	REDIS_CHANNEL_SLOW   = "kd-slowChannel"
	REDIS_CHANNEL_ERROR  = "kd-errorChannel"

	REDIS_STREAM_REPORT = "kd-reportStream"
	REDIS_STREAM_GROUP  = "kd-reportGroup"

	REDIS_KEY_SAMPLE      = "kd-sample-value"
	REDIS_KEY_SAMPLE_TIME = "kd-sample-time"
	REDIS_KEY_SAMPLE_LOCK = "kd-sample-lock"
)

var (
	consumerName = fmt.Sprintf("consumer-%d-%d", time.Now().UnixNano(), rand.Intn(100))
)

func (client *RedisClient) Start() {
	_, err := client.rdb.XGroupCreateMkStream(context.Background(), REDIS_STREAM_REPORT, REDIS_STREAM_GROUP, "$").Result()
	if err != nil && err.Error() != "Consumer Group name already exists" {
		log.Printf("Error create Consume Group: %v", err)
	}
}

func (cache *RedisClient) IsLocal() bool {
	return false
}

// ========== Metric ==========
/*
	kd-onoff-metric-<traceId>, ExpireTime: 60s
*/
func (client *RedisClient) StoreMetric(metric *model.OnOffMetricGroup, json string) {
	client.storeList(fmt.Sprintf(REDIS_KEY_METRIC, metric.TraceId), json)
}

/*
kd-onoff-metric-<traceId>
*/
func (client *RedisClient) GetMetricSize(traceId string) int {
	return int(client.getListSize(fmt.Sprintf(REDIS_KEY_METRIC, traceId)))
}

/*
kd-onoff-metric-<traceId>
*/
func (client *RedisClient) GetMetrics(traceId string) []*model.OnOffMetricGroup {
	metrics := make([]*model.OnOffMetricGroup, 0)
	metricJsons := client.getList(fmt.Sprintf(REDIS_KEY_METRIC, traceId), -1)
	for _, metricJson := range metricJsons {
		onOffMetricGroup := &model.OnOffMetricGroup{}
		if err := json.Unmarshal([]byte(metricJson), onOffMetricGroup); err == nil {
			metrics = append(metrics, onOffMetricGroup)
		}
	}
	return metrics
}

// ========== Trace ==========
/*
	kd-span-trace-<traceId>, ExpireTime: 60s
*/
func (client *RedisClient) StoreTrace(trace *model.Trace, json string) {
	client.storeList(fmt.Sprintf(REDIS_KEY_TRACE, trace.Labels.TraceId), json)
}

/*
kd-last-trace-<traceId>
*/
func (client *RedisClient) RecordTraceTime(traceId string, data int64) {
	client.setInt(fmt.Sprintf(REDIS_KEY_LAST_TRACE, traceId), data)
}

func (cache *RedisClient) GetTraceTime(traceId string) int64 {
	return cache.getInt(fmt.Sprintf(REDIS_KEY_LAST_TRACE, traceId))
}

/*
kd-span-trace-<traceId>
*/
func (client *RedisClient) GetTraceSize(traceId string) int {
	return int(client.getListSize(fmt.Sprintf(REDIS_KEY_TRACE, traceId)))
}

/*
kd-span-trace-<traceId>
*/
func (client *RedisClient) GetTraces(traceId string) []*model.Trace {
	traces := make([]*model.Trace, 0)
	traceJsons := client.getList(fmt.Sprintf(REDIS_KEY_TRACE, traceId), -1)
	for _, traceJson := range traceJsons {
		trace := &model.Trace{Labels: &model.TraceLabels{ThresholdMultiple: 1.0}}
		if err := json.Unmarshal([]byte(traceJson), trace); err == nil {
			traces = append(traces, trace)
		}
	}
	return traces
}

/*
kd-reportStream
*/
func (client *RedisClient) NotifyReportTraceId(traceId string) {
	client.xAddChannel(REDIS_STREAM_REPORT, traceId)
}

func (client *RedisClient) SubscribeReportTraceId(subscriber Subscriber) {
	for {
		client.xReadGroup(REDIS_STREAM_GROUP, consumerName, REDIS_STREAM_REPORT, subscriber)
	}
}

func (client *RedisClient) GetTraceIndex() int64 {
	return client.getNo(REDIS_KEY_TRACE_INDEX)
}

func (client *RedisClient) IncrTraceIndex() int64 {
	return client.incr(REDIS_KEY_TRACE_INDEX)
}

func (client *RedisClient) NotifySampledTraceIds(normalTraceIds []string, slowTraceIds []string, errorTraceIds []string) {
	for _, normalTraceId := range normalTraceIds {
		// kd-normalChannel
		client.pubChannel(REDIS_CHANNEL_NORMAL, normalTraceId)
	}
	for _, slowTraceId := range slowTraceIds {
		// kd-slowChannel
		client.pubChannel(REDIS_CHANNEL_SLOW, slowTraceId)
	}
	for _, errorTraceId := range errorTraceIds {
		// kd-errorChannel
		client.pubChannel(REDIS_CHANNEL_ERROR, errorTraceId)
	}
}

func (client *RedisClient) SubscribeTraceIds(normalSubscriber Subscriber, slowSubscriber Subscriber, errorSubscriber Subscriber) {
	client.subscribeChannel(REDIS_CHANNEL_NORMAL, normalSubscriber)
	client.subscribeChannel(REDIS_CHANNEL_SLOW, slowSubscriber)
	client.subscribeChannel(REDIS_CHANNEL_ERROR, errorSubscriber)
}

/*
kd-signal-<traceId>
*/
func (client *RedisClient) StoreSignal(nodeIp string, json string) {
	client.storeList(fmt.Sprintf(REDIS_KEY_SIGNAL, nodeIp), json)
}

func (client *RedisClient) GetAndCleanSignals(nodeIp string) []string {
	key := fmt.Sprintf(REDIS_KEY_SIGNAL, nodeIp)
	size := client.getListSize(key)
	if size > 0 {
		result := client.getList(key, size)
		client.ltrimList(key, size)
		return result
	}
	return nil
}

// ========== Relation ==========
func (client *RedisClient) StoreRelationTraceId(key string, traceId string) {
	client.set(fmt.Sprintf(REDIS_KEY_SENT_RELATION, key), traceId, 3600)
}

func (client *RedisClient) GetRelationTraceId(key string) string {
	return client.get(fmt.Sprintf(REDIS_KEY_SENT_RELATION, key))
}

// SampleValue
func (client *RedisClient) GetSampleValue() int64 {
	return client.getInt(REDIS_KEY_SAMPLE)
}

func (client *RedisClient) InitSampleValue(sampleValue int64, expirePeriod int64) {
	if client.setNxIntWithExpireTime(REDIS_KEY_SAMPLE, sampleValue, expirePeriod*2) {
		client.setIntWithExpireTime(REDIS_KEY_SAMPLE_TIME, sampleValue, expirePeriod)
	}
}

func (client *RedisClient) SetSampleValue(sampleValue int64, expirePeriod int64) {
	client.setIntWithExpireTime(REDIS_KEY_SAMPLE, sampleValue, expirePeriod*2)
	client.setIntWithExpireTime(REDIS_KEY_SAMPLE_TIME, sampleValue, expirePeriod)
}

func (client *RedisClient) LockAndCheckSampleTime() bool {
	if client.setNxIntWithExpireTime(REDIS_KEY_SAMPLE_LOCK, 0, 2) {
		return !client.has(REDIS_KEY_SAMPLE_TIME)
	}
	return false
}
