package wscli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type WsCli interface {
	Push(ctx context.Context, event string, message []byte) (*http.Response, error)
	Pull(ctx context.Context) (message []byte, err error)
}

type wsCli struct {
	http.Client
	u func() (*url.URL, error)
}

func NewWsCli(model string, fn func() (string, uint64, error), dviceHost string, devicePort uint64) *wsCli {

	wsCli := &wsCli{
		Client: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Second,
		},
		u: func() (*url.URL, error) {
			ip, port := "", uint64(0)
			return &url.URL{
				Scheme: "http",
				Host:   ip + ":" + strconv.FormatUint(port, 10),
				Path:   "/notice",
			}, nil
		},
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, event string, req Request) (*http.Response, error) {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	switch event {
	case "queue":
		// todo
		return nil, nil
	case "http":
		deviceUrl, err := d.u()
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
