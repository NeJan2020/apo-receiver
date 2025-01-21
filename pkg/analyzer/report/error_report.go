package report

import (
	"time"

	"github.com/CloudDetail/apo-module/model/v1"
)

type ErrorReport struct {
	Name      string           `json:"name"`
	IndexTime int64            `json:"index_time"`
	Timestamp uint64           `json:"timestamp"`
	TraceId   string           `json:"trace_id"`
	IsDrop    bool             `json:"is_drop"`
	Duration  uint64           `json:"duration"`
	Data      *ErrorReportData `json:"data"`
}

type ErrorReportData struct {
	EndTime            uint64 `json:"end_time,omitempty"`
	MutatedContainerId string `json:"mutated_container_id,omitempty"`
	DropReason         string `json:"drop_reason,omitempty"`

	model.ErrorReportData
}

func NewErrorReport(timestamp uint64, traceId string, duration uint64, data *ErrorReportData) *ErrorReport {
	return &ErrorReport{
		Name:      CameraErrorReport,
		IndexTime: time.Now().UnixMilli() * 1e6,
		Timestamp: timestamp,
		TraceId:   traceId,
		IsDrop:    false,
		Duration:  duration,
		Data:      data,
	}
}

func NewDropErrorReport(name string, trace *model.Trace, errorMsg string) *ErrorReport {
	entry := trace.Labels
	return &ErrorReport{
		Name:      name,
		IndexTime: time.Now().UnixMilli() * 1e6,
		Timestamp: entry.StartTime,
		TraceId:   entry.TraceId,
		IsDrop:    true,
		Duration:  entry.Duration,
		Data: &ErrorReportData{
			EndTime:    entry.EndTime,
			DropReason: errorMsg,
			ErrorReportData: model.ErrorReportData{
				EntryService:  entry.ServiceName,
				EntryInstance: trace.GetInstanceId(),
				ContentKey:    entry.Url,
			},
		},
	}
}
