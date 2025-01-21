package external

import (
	"fmt"
	"strings"

	"github.com/CloudDetail/apo-module/apm/model/v1"
)

type httpParser struct {
	urlParser string
}

func newHttpParser(urlParser string) *httpParser {
	return &httpParser{
		urlParser: urlParser,
	}
}

func (http *httpParser) Parse(span *model.OtelSpan) *External {
	if span.Kind != model.SpanKindClient {
		return nil
	}
	httpMethod := span.GetHttpMethod()
	if httpMethod == "" {
		return nil
	}

	url := span.GetHttpDetail()
	name := httpMethod
	if http.urlParser == "topUrl" {
		name = fmt.Sprintf("%s %s", httpMethod, GetTopUrl(url))
	}
	return newExternal(span).
		WithGroup(GroupExternal).
		WithType("http").
		WithName(name).
		WithPeer(span.GetPeer("")).
		WithDetail(url)
}

func GetTopUrl(url string) string {
	urls := strings.Split(url, "/")
	if len(urls) == 1 {
		return url
	}
	if len(urls) >= 3 && strings.HasSuffix(urls[0], ":") && urls[1] == "" {
		// schema://host:port/path
		if len(urls) == 3 {
			return "/"
		} else {
			return fmt.Sprintf("/%s", urls[3])
		}
	}
	return fmt.Sprintf("/%s", urls[1])
}
