package trace

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CloudDetail/apo-receiver/pkg/global"
	"github.com/CloudDetail/apo-receiver/pkg/model"
)

type MemorySampler struct {
	MinSample    int64
	InitSample   int64
	MaxSample    int64
	SampleValue  *atomic.Int64
	NodeMemories sync.Map // <nodeIp, NodeMemories>
	ResetPeriod  int64
}

func NewMemorySampler(minSample int64, initSample int64, maxSample int64, resetPeriod int64) *MemorySampler {
	sampleValue := &atomic.Int64{}
	sampleValue.Store(int64(minSample))
	global.CACHE.InitSampleValue(minSample, resetPeriod)

	return &MemorySampler{
		MinSample:   minSample,
		InitSample:  initSample,
		MaxSample:   maxSample,
		SampleValue: sampleValue,
		ResetPeriod: resetPeriod,
	}
}

func (sampler *MemorySampler) GetSampleValue(metric *model.SampleMetric) *model.SampleResult {
	var nodeMemories *NodeMemories
	if cachedMemories, ok := sampler.NodeMemories.Load(metric.NodeIp); ok {
		nodeMemories = cachedMemories.(*NodeMemories)
	} else {
		nodeMemories = NewNodeMemories(5)
		sampler.NodeMemories.Store(metric.NodeIp, nodeMemories)
	}
	nodeMemories.CacheMemory(metric)

	return &model.SampleResult{
		Value: sampler.SampleValue.Load(),
	}
}

func (sampler *MemorySampler) CalcSampleValue() {
	timer := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-timer.C:
			sampler.CheckSampleValue()
		}
	}
}

func (sampler *MemorySampler) CheckSampleValue() {
	exceedLimit := false
	sampleChanged := false

	sampleValue := global.CACHE.GetSampleValue()
	localSampleValue := sampler.SampleValue.Load()
	if sampleValue != localSampleValue {
		sampleChanged = true
		sampler.SampleValue.Store(sampleValue)
		log.Printf("[Update SampleValue] %d => %d", localSampleValue, sampleValue)
	} else if sampleValue < sampler.MaxSample {
		sampler.NodeMemories.Range(func(k, v interface{}) bool {
			exceedMemoryLimit, sampled := v.(*NodeMemories).SetNewSampleValue()
			if sampled {
				sampleChanged = true
			}
			if exceedMemoryLimit {
				exceedLimit = true
			}
			return true
		})

		if exceedLimit && !sampleChanged && sampleValue < sampler.InitSample {
			// 1 / 16
			sampleValue = sampler.InitSample
			sampleChanged = true
		} else if sampleChanged && sampleValue <= sampler.MaxSample {
			sampleValue += 1
		}

		if sampleChanged {
			sampler.SampleValue.Store(sampleValue)
			global.CACHE.SetSampleValue(sampleValue, sampler.ResetPeriod)

			log.Printf("[Set SampleValue] %d => %d", localSampleValue, sampleValue)
		}
	}

	if global.CACHE.LockAndCheckSampleTime() {
		if sampleValue > sampler.MinSample {
			sampleValue -= 1
			sampleChanged = true
		}
		sampler.SampleValue.Store(sampleValue)
		global.CACHE.SetSampleValue(sampleValue, sampler.ResetPeriod)

		log.Printf("[Recover SampleValue] %d => %d", localSampleValue, sampleValue)
	}

	if sampleChanged {
		sampler.NodeMemories.Range(func(k, v interface{}) bool {
			v.(*NodeMemories).ResetCheckCount()
			return true
		})
	}
}

type NodeMemories struct {
	lock        sync.Mutex
	Size        int
	Memories    []*NodeMemory
	CheckTime   int64
	CheckCount  int
	SampleCount int
}

func NewNodeMemories(size int) *NodeMemories {
	return &NodeMemories{
		Size:        size,
		Memories:    make([]*NodeMemory, 0),
		CheckCount:  0,
		SampleCount: 0,
	}
}

func (memories *NodeMemories) CacheMemory(metric *model.SampleMetric) {
	memories.lock.Lock()
	defer memories.lock.Unlock()

	memories.Memories = append(memories.Memories, &NodeMemory{
		Timestamp:   metric.QueryTime,
		Memory:      metric.Memory,
		MemoryLimit: metric.MemoryLimit,
		CacheSecond: metric.CacheSecond,
	})
	if len(memories.Memories) > memories.Size {
		memories.Memories = memories.Memories[1:]
	}
}

func (memories *NodeMemories) SetNewSampleValue() (exceedLimit bool, sampled bool) {
	if len(memories.Memories) < 2 {
		return
	}

	memories.lock.Lock()
	defer memories.lock.Unlock()
	size := len(memories.Memories)
	last := memories.Memories[size-1]

	memoryLimit := last.MemoryLimit
	// 0.8
	if memories.CheckTime == last.Timestamp || last.Memory*5 < memoryLimit*4 {
		return
	}

	memories.CheckTime = last.Timestamp

	pre := memories.Memories[size-2]
	first := memories.Memories[0]
	lastMemory := int64(last.Memory-pre.Memory) / (last.Timestamp - pre.Timestamp)
	last5Memory := int64(last.Memory-first.Memory) / (last.Timestamp - first.Timestamp)
	avgMemory := int64(memoryLimit) / last.CacheSecond
	if lastMemory > avgMemory || last5Memory > avgMemory {
		memories.SampleCount += 1
	}

	exceedLimit = true
	sampled = memories.SampleCount >= 5
	if memories.SampleCount >= 5 || memories.CheckCount > 10 {
		memories.SampleCount = 0
		memories.CheckCount = 0
	}
	return
}

func (memories *NodeMemories) ResetCheckCount() {
	memories.lock.Lock()
	defer memories.lock.Unlock()

	memories.SampleCount = 0
	memories.CheckCount = 0
}

type NodeMemory struct {
	Timestamp   int64
	Memory      uint64
	MemoryLimit uint64
	CacheSecond int64
}
