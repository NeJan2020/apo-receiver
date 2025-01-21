package model

type SlowReportCountMetric struct {
	Name           string `json:"name"`
	Timestamp      int64  `json:"timestamp"`
	EntryService   string `json:"entry_service"`
	EntryUrl       string `json:"entry_url"`
	MutatedService string `json:"mutated_service"`
	MutatedUrl     string `json:"mutated_url"`
	Total          int    `json:"total"`
	Success        int    `json:"success"`
}
