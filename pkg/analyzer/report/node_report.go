package report

import (
	"time"

	"github.com/CloudDetail/apo-module/model/v1"
)

type NodeReport struct {
	Name      string `json:"name"`
	IndexTime int64  `json:"index_time"`
	IsDrop    bool   `json:"is_drop"`

	// Basic Node Report define in module
	Timestamp uint64 `json:"timestamp"`
	TraceId   string `json:"trace_id"`
	Duration  uint64 `json:"duration"`
	// overwrite Data struct
	Data *ReportData `json:"data"`
}

type ReportData struct {
	EndTime    uint64 `json:"end_time,omitempty"`
	DropReason string `json:"drop_reason,omitempty"`

	model.CameraNodeReportData
}

func NewDropReport(name string, trace *model.Trace, errorMsg string) *NodeReport {
	entry := trace.Labels
	return &NodeReport{
		Name:      name,
		IndexTime: time.Now().UnixMilli() * 1e6,
		Timestamp: entry.StartTime,
		TraceId:   entry.TraceId,
		IsDrop:    true,
		Duration:  entry.Duration,
		Data: &ReportData{
			EndTime:    entry.EndTime,
			DropReason: errorMsg,
			CameraNodeReportData: model.CameraNodeReportData{
				EntryService:  entry.ServiceName,
				EntryInstance: trace.GetInstanceId(),
				ContentKey:    entry.Url,
			},
		},
	}
}

func NewNodeReport(timestamp uint64, traceId string, duration uint64, data *ReportData) *NodeReport {
	return &NodeReport{
		Name:      CameraNodeReport,
		IndexTime: time.Now().UnixMilli() * 1e6,
		Timestamp: timestamp,
		TraceId:   traceId,
		IsDrop:    false,
		Duration:  duration,
		Data:      data,
	}
}
