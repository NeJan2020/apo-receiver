package external

import (
	"github.com/CloudDetail/apo-module/apm/model/v1"
)

const (
	GroupExternal = "external"
	GroupDb       = "db"
	GroupMq       = "mq"
)

type ExternalFactory struct {
	dbParser      ExternalParser
	httpParser    ExternalParser
	rpcParser     ExternalParser
	unknownParser ExternalParser
	mqParser      ExternalParser
}

func NewExternalFactory(urlParser string) *ExternalFactory {
	return &ExternalFactory{
		dbParser:      newDbParser(),
		httpParser:    newHttpParser(urlParser),
		rpcParser:     newRpcParser(),
		unknownParser: newUnknownParser(),
		mqParser:      newMqParser(),
	}
}

func (factory *ExternalFactory) BuildExternals(service *model.OtelServiceNode) []*External {
	externals := make([]*External, 0)
	for _, entrySpan := range service.EntrySpans {
		if external := factory.buildExternal(entrySpan); external != nil {
			externals = append(externals, external)
		}
	}
	for _, exitSpan := range service.ExitSpans {
		if external := factory.buildExternal(exitSpan); external != nil {
			externals = append(externals, external)
		}
	}
	return externals
}

func (factory *ExternalFactory) buildExternal(span *model.OtelSpan) *External {
	if span.Kind == model.SpanKindClient {
		if external := factory.dbParser.Parse(span); external != nil {
			return external
		}
		if external := factory.httpParser.Parse(span); external != nil {
			return external
		}
		if external := factory.rpcParser.Parse(span); external != nil {
			return external
		}
		if _, mqFound := span.Attributes[model.AttributeMessageSystem]; mqFound {
			return factory.mqParser.Parse(span)
		} else {
			_, dbFound := span.Attributes[model.AttributeDBSystem]
			if !dbFound {
				return factory.unknownParser.Parse(span)
			}
		}
	} else if span.Kind == model.SpanKindConsumer || span.Kind == model.SpanKindProducer {
		return factory.mqParser.Parse(span)
	}
	return nil
}
