package external

import (
	"github.com/CloudDetail/apo-module/apm/model/v1"
)

type rpcParser struct {
}

func newRpcParser() *rpcParser {
	return &rpcParser{}
}

func (rpc *rpcParser) Parse(span *model.OtelSpan) *External {
	if span.Kind != model.SpanKindClient {
		return nil
	}
	rpcSystem, rpcFound := span.Attributes[model.AttributeRpcSystem]
	if !rpcFound {
		return nil
	}

	return newExternal(span).
		WithGroup(GroupExternal).
		WithType(rpcSystem).
		WithName(span.Name).
		WithPeer(span.GetPeer("")).
		WithDetail(span.GetRpcDetail(span.Name))
}
