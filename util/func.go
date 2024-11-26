// Package util @author: Violet-Eva @date  : 2024/11/25 @notes :
package util

import (
	"net/http"
	"net/url"
)

func SetRequestBasicAuth(request *http.Request, username string, password string) {
	request.SetBasicAuth(username, password)
}

func SetRequestHeader(request *http.Request, mssArr ...map[string]string) {
	for _, mss := range mssArr {
		for key, value := range mss {
			request.Header.Set(key, value)
		}
	}
}

func GetResponse(request *http.Request, proxy string) (resp *http.Response, err error) {

	var (
		proxyUrl *url.URL
	)

	if proxy != "" {
		proxyUrl, err = url.Parse(proxy)
		if err != nil {
			return
		}
		resp, err = (&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyUrl),
			},
		}).Do(request)
	} else {
		resp, err = (&http.Client{}).Do(request)
	}

	return
}
