package threshold

import (
	"context"
	"github.com/CloudDetail/apo-receiver/pkg/model"
)

type Server struct {
	model.UnimplementedSlowThresholdServiceServer
	thresholdCache *ThresholdCache
}

func NewThresholdServer(thresholdCache *ThresholdCache) *Server {
	return &Server{
		thresholdCache: thresholdCache,
	}
}

func (s *Server) QuerySlowThreshold(ctx context.Context, request *model.SlowThresholdRequest) (*model.SlowThresholdResponse, error) {
	// Get the array of slow threshold values
	response := make([]*model.SlowThresholdData, 0)
	localCache := s.thresholdCache.SlowThresholdMap
	for _, v := range localCache {
		response = append(response, v)
	}

	exceptions := make([]*model.ExceptionSwitchData, 0)
	return &model.SlowThresholdResponse{
		Datas:      response,
		Exceptions: exceptions,
	}, nil
}
