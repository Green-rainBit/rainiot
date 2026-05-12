package devicecli

import (
	"net/http"
	"net/url"
)

type deviceCli struct {
}

func NewDeviceCli(model string, fn func() (string, uint64, error), dviceHost string, devicePort uint64) func() (http.Client, error) {
	return func() (http.Client, error) {
		ip, port := "", uint64(0)
		var err error
		if model == "nacos" {
			ip, port, err = fn()
			if err != nil {
				return http.Client{}, err
			}
		}
		if err != nil {
			return http.Client{}, err
		}
		if ip == "" && port == 0 {
			ip, port = dviceHost, devicePort
		}
		httpClient := http.Client{
			Transport: &http.Transport{
				Proxy: func(req *http.Request) (*url.URL, error) {
					return http.ProxyURL(&url.URL{
						Scheme: "http",
						Host:   ip + ":" + string(port),
					})(req)
				},
			},
		}

		return httpClient, nil
	}
}
