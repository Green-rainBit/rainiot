package devicecli

import (
	"context"

	"rainiot/pkg/devicecli/httpc"
)

type DeviceCli interface {
	Push(ctx context.Context, event string, message []byte) ([]byte, error)
	// Pull(ctx context.Context) (message []byte, err error)
}

type deviceCli struct {
	http DeviceCli
}

func NewDeviceCli(model, serviceName string, fn func(serviceName string) []string) DeviceCli {
	return &deviceCli{
		http: httpc.NewDeviceCli(model, serviceName, fn),
	}
}

func (d *deviceCli) Push(ctx context.Context, event string, message []byte) ([]byte, error) {
	return d.http.Push(ctx, event, message)
}
