package external

import "github.com/CloudDetail/apo-module/apm/model/v1"

type mqParser struct {
}

func newMqParser() *mqParser {
	return &mqParser{}
}

func (mq *mqParser) Parse(span *model.OtelSpan) *External {
	if span.Kind != model.SpanKindClient && span.Kind != model.SpanKindConsumer && span.Kind != model.SpanKindProducer {
		return nil
	}

	mqSystem, mqFound := span.Attributes[model.AttributeMessageSystem]
	if !mqFound {
		if span.Kind == model.SpanKindClient {
			return nil
		} else {
			mqSystem = Unknown
		}
	}

	var (
		name   string
		detail string
	)
	if span.Kind == model.SpanKindClient {
		name = span.Name
		detail = span.Attributes[model.AttributeMessageDestinationName]
	} else {
		name = span.GetMessageDestination(Unknown)
		detail = span.Name
	}

	return newExternal(span).
		WithGroup(GroupMq).
		WithType(mqSystem).
		WithName(name).
		WithPeer(span.GetPeer("")).
		WithDetail(detail)
}
