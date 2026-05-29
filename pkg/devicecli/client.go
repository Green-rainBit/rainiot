package devicecli

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type DeviceCli interface {
	Push(ctx context.Context, event string, message []byte) (*http.Response, error)
	Pull(ctx context.Context) (message []byte, err error)
}

type deviceCli struct {
	serviceName string
	http.Client
	u func() (*url.URL, error)
}

func NewDeviceCli(model, serviceName string, fn func() (string, uint64, error), dviceHost string, devicePort uint64) DeviceCli {

	deviceCli := &deviceCli{
		serviceName: serviceName,
		Client: http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
			},
			Timeout: 5 * time.Second,
		},
		u: func() (*url.URL, error) {
			ip, port := "", uint64(0)
			var err error
			if model == "nacos" {
				ip, port, err = fn()
				if err != nil {
					return nil, err
				}
			}
			if err != nil {
				return nil, err
			}
			if ip == "" && port == 0 {
				ip, port = dviceHost, devicePort
			}

			return &url.URL{
				Scheme: "http",
				Host:   ip + ":" + strconv.FormatUint(port, 10),
				Path:   "/device/connect",
			}, nil
		},
	}
	return deviceCli
}

func (d *deviceCli) Push(ctx context.Context, event string, message []byte) (*http.Response, error) {
	switch event {
	case "queue":
		// todo
		return nil, nil
	case "http":
		deviceUrl, err := d.u()
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(http.MethodPost, deviceUrl.String(), bytes.NewBuffer(message))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("ServiceName", d.serviceName)
		req.Header.Set("CoonId", d.serviceName)
		return d.Do(req)
	}
	return nil, nil
}
func (d *deviceCli) Pull(ctx context.Context) (message []byte, err error) {

	return nil, nil
}
