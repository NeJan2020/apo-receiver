package httphelper

import (
	"log"
	"net/http"
	"net/url"
)

func CreateHttpClient(proxy bool, proxyAddress string) *http.Client {
	transport := &http.Transport{}
	if proxy {
		proxyURL, err := url.Parse(proxyAddress)
		if err != nil {
			log.Printf("warning: no proxy will be used since cannot parse the proxy address %v", err)
		} else {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	return &http.Client{
		Transport: transport,
	}
}
