package trace

import (
	"context"
	"time"

	"github.com/CloudDetail/apo-receiver/pkg/model"
)

type SampleServer struct {
	model.UnimplementedSampleServiceServer
	enable  bool
	sampler *MemorySampler
}

func NewSampleServer(enable bool, minSample int64, initSample int64, maxSample int64, resetPeriod time.Duration) *SampleServer {
	return &SampleServer{
		enable:  enable,
		sampler: NewMemorySampler(minSample, initSample, maxSample, int64(resetPeriod.Seconds())),
	}
}

func (server *SampleServer) GetSampleValue(ctx context.Context, metric *model.SampleMetric) (*model.SampleResult, error) {
	if server.enable {
		return server.sampler.GetSampleValue(metric), nil
	}
	return &model.SampleResult{
		Value: 0,
	}, nil
}

func (server *SampleServer) Start() {
	if server.enable {
		go server.sampler.CalcSampleValue()
	}
}
