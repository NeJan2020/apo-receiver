package external

import "github.com/CloudDetail/apo-module/apm/model/v1"

type External struct {
	StartTime  uint64
	Duration   uint64
	NextSpanId string
	PSpanId    string
	SpanId     string
	Group      string
	Type       string
	Kind       model.OtelSpanKind
	Name       string
	Peer       string
	Error      bool
	Detail     string
}

func newExternal(span *model.OtelSpan) *External {
	return &External{
		StartTime:  span.StartTime,
		Duration:   span.Duration,
		SpanId:     span.SpanId,
		NextSpanId: span.NextSpanId,
		PSpanId:    span.PSpanId,
		Kind:       span.Kind,
		Error:      span.IsError(),
	}
}

func (external *External) WithGroup(clientGroup string) *External {
	external.Group = clientGroup
	return external
}

func (external *External) WithType(clientType string) *External {
	external.Type = clientType
	return external
}

func (external *External) WithName(clientName string) *External {
	external.Name = clientName
	return external
}

func (external *External) WithPeer(clientPeer string) *External {
	external.Peer = clientPeer
	return external
}

func (external *External) WithDetail(clientDetail string) *External {
	external.Detail = clientDetail
	return external
}

// For test
func NewExternal(
	clientTime uint64,
	clientDuration uint64,
	nextSpanId string,
	clientSpanId string,
	clientGroup string,
	clientSystem string,
	clientKind model.OtelSpanKind,
	clientName string,
	clientPeer string,
	clientError bool,
	clientDetail string) *External {
	return &External{
		StartTime:  clientTime,
		Duration:   clientDuration,
		NextSpanId: nextSpanId,
		SpanId:     clientSpanId,
		Group:      clientGroup,
		Type:       clientSystem,
		Kind:       clientKind,
		Name:       clientName,
		Peer:       clientPeer,
		Error:      clientError,
		Detail:     clientDetail,
	}
}

type ExternalParser interface {
	Parse(span *model.OtelSpan) *External
}
