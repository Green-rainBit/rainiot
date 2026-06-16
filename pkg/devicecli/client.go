package devicecli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"rainiot/pkg/devicecli/pb"
	"strconv"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
)

type DeviceCli interface {
	Push(ctx context.Context, event string, message []byte) ([]byte, error)
	Pull(ctx context.Context) (message []byte, err error)
}

type deviceCli struct {
	serviceName        string
	rpcIotdeviceClient pb.IotdeviceClient
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

func (d *deviceCli) Push(ctx context.Context, event string, message []byte) ([]byte, error) {
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
		req.Header.Set("ConId", d.serviceName)
		resp, err := d.Do(req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	case "grpc":
		req := &pb.DeviceConnectReq{}
		if err := protojson.Unmarshal(message, req); err != nil {
			panic(err)
		}
		resp, err := d.rpcIotdeviceClient.DeviceConnect(ctx, req)
		if err != nil {
			return nil, err
		}
		return []byte(resp.GetMessage()), nil

	}
	return nil, nil
}
func (d *deviceCli) Pull(ctx context.Context) (message []byte, err error) {

	return nil, nil
}
