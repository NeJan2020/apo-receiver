package profile

import (
	"sync"
	"time"

	"github.com/CloudDetail/apo-receiver/pkg/global"
)

type traceIdCache struct {
	name     string
	traceIds sync.Map // <traceId, time>
	timeout  int64
}

func NewTraceIdCache(name string, timeout int) *traceIdCache {
	return &traceIdCache{
		name:    name,
		timeout: int64(timeout) * int64(time.Second),
	}
}

func (cache *traceIdCache) Consume(traceId string) {
	if _, ok := cache.traceIds.Load(traceId); !ok {
		index := global.CACHE.IncrTraceIndex()
		if index > 0 {
			cache.traceIds.Store(traceId, newTraceIdTime(index, cache.timeout))
		}
	}
}

func (cache *traceIdCache) cleanExpireTraceId(now int64) {
	cache.traceIds.Range(func(k, v any) bool {
		time := v.(*traceIdTime)
		if now > time.expireTime {
			cache.traceIds.Delete(k)
		}
		return true
	})
}

func (cache *traceIdCache) getTraceIds(ignoreTraces map[string]bool, startIndex int64, endIndex int64) []string {
	traceIds := make([]string, 0)
	if endIndex == -1 || startIndex == endIndex {
		return traceIds
	}
	cache.traceIds.Range(func(k, v any) bool {
		time := v.(*traceIdTime)
		traceId := k.(string)
		if time.index > startIndex && time.index <= endIndex {
			if _, exist := ignoreTraces[traceId]; !exist {
				traceIds = append(traceIds, traceId)
			}
		}
		return true
	})
	return traceIds
}

type traceIdTime struct {
	index      int64
	expireTime int64
}

func newTraceIdTime(index int64, timeout int64) *traceIdTime {
	return &traceIdTime{
		index:      index,
		expireTime: time.Now().UnixNano() + timeout,
	}
}
