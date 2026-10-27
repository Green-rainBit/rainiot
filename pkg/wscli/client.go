package wscli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"rainiot/pkg/configcli"
)

type WsCli interface {
	Ping(ctx context.Context, event string, message []byte) (*http.Response, error)
}

type wsCli struct {
	httpCli httpc.Client
}

func NewWsCli(confCli configcli.ConfigCli) *wsCli {
	wsCli := &wsCli{
		confCli: confCli,
		Client: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Second,
		},
		// u: func() (*url.URL, error) {
		// 	ip, port := "", uint64(0)
		// 	return &url.URL{
		// 		Scheme: "http",
		// 		Host:   ip + ":" + strconv.FormatUint(port, 10),
		// 		Path:   "/notice",
		// 	}, nil
		// },
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, serviceName, event string, req Request) (*http.Response, error) {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	switch event {
	case "queue":
		// todo
		return nil, nil
	case "http":
		deviceUrl := d.httpCli.GetHealthyInstances(serviceName)
		if err != nil {
			return nil, err
		}
		return d.Post(deviceUrl.String(), "application/json", bytes.NewBuffer(jsonBytes))
	}
	return nil, nil
}
func (d *wsCli) Pull(ctx context.Context) (message []byte, err error) {

	return nil, nil
}
