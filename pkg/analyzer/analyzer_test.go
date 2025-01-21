package analyzer

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/CloudDetail/apo-module/model/v1"
	"github.com/CloudDetail/apo-receiver/pkg/analyzer/report"
)

type ReportDataE struct {
	report.ReportData

	RelationP *model.TraceTreeNode
}

func newNodeReportE(timestamp uint64, traceId string, duration uint64, data *ReportDataE) *NodeReportE {
	return &NodeReportE{
		Name:      report.CameraNodeReport,
		Timestamp: timestamp,
		TraceId:   traceId,
		IsDrop:    false,
		Duration:  duration,
		Data:      data,
	}
}

func BenchmarkToStrToJson(b *testing.B) {
	relationPath := &model.TraceTreeNode{
		Id:           "1234567890",
		Url:          "POST:/12312ukjksdfksbfn",
		StartTime:    9999,
		TotalTime:    9999,
		P90:          9999,
		IsTraced:     false,
		IsProfiled:   false,
		IsPath:       false,
		IsMutated:    false,
		SelfTime:     9999,
		SelfP90:      9999,
		MutatedValue: 9999,
		SpanId:       "0987654321",
		Children: []*model.TraceTreeNode{
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
		},
	}

	var dataStr string
	// TODO: Initialize
	for i := 0; i < b.N; i++ {
		// TODO: Your Code Here
		relationPathStr, _ := json.Marshal(relationPath)
		data := &report.ReportData{
			EndTime: 2345,
			CameraNodeReportData: model.CameraNodeReportData{
				EntryService:   "12345678",
				MutatedService: "12345678",
				MutatedSpan:    "12345678",
				ContentKey:     "POST:/12312ukjksdfksbfn",
				Relation:       string(relationPathStr),
				ClientCalls:    "123456",
			},
		}

		report := report.NewNodeReport(1234, "12345678", 1234, data)
		reportJson, err := json.Marshal(report)
		if err != nil {
			dataStr = string(reportJson)
		}
	}

	if len(dataStr) > 0 {
		fmt.Println("ok")
	}
}

type NodeReportE struct {
	Name      string       `json:"name"`
	Timestamp uint64       `json:"timestamp"`
	TraceId   string       `json:"trace_id"`
	IsDrop    bool         `json:"is_drop"`
	Duration  uint64       `json:"duration"`
	Data      *ReportDataE `json:"data"`
}

func BenchmarkDirectToJson(b *testing.B) {
	relationPath := &model.TraceTreeNode{
		Id:           "1234567890",
		Url:          "POST:/12312ukjksdfksbfn",
		StartTime:    9999,
		TotalTime:    9999,
		P90:          9999,
		IsTraced:     false,
		IsProfiled:   false,
		IsPath:       false,
		IsMutated:    false,
		SelfTime:     9999,
		SelfP90:      9999,
		MutatedValue: 9999,
		SpanId:       "0987654321",
		Children: []*model.TraceTreeNode{
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
			{
				Id:           "1234567890",
				Url:          "POST:/12312ukjksdfksbfn",
				StartTime:    9999,
				TotalTime:    9999,
				P90:          9999,
				IsTraced:     false,
				IsProfiled:   false,
				IsPath:       false,
				IsMutated:    false,
				SelfTime:     9999,
				SelfP90:      9999,
				MutatedValue: 9999,
				SpanId:       "0987654321",
				Children:     nil,
			},
		},
	}

	var dataStr string
	// TODO: Initialize
	for i := 0; i < b.N; i++ {
		// TODO: Your Code Here
		data := &ReportDataE{
			ReportData: report.ReportData{
				EndTime: 2345,
				CameraNodeReportData: model.CameraNodeReportData{
					EntryService:   "12345678",
					MutatedService: "12345678",
					MutatedSpan:    "12345678",
					ContentKey:     "POST:/12312ukjksdfksbfn",
					Relation:       "",
					ClientCalls:    "123456",
				},
			},
			RelationP: relationPath,
		}

		report := report.NewNodeReport(1234, "12345678", 1234, &data.ReportData)
		reportJson, err := json.Marshal(report)
		if err != nil {
			dataStr = string(reportJson)
		}
	}

	if len(dataStr) > 0 {
		fmt.Println("ok")
	}
}
