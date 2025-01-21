package external

import "github.com/CloudDetail/apo-module/apm/model/v1"

const (
	Unknown = "unknown"
)

type unknownParser struct {
}

func newUnknownParser() *unknownParser {
	return &unknownParser{}
}

func (unknown *unknownParser) Parse(span *model.OtelSpan) *External {
	return newExternal(span).
		WithGroup(GroupExternal).
		WithType(Unknown).
		WithName(span.Name).
		WithPeer(span.GetPeer(""))
}
