package devicecli

import (
	"context"

	"rainiot/pkg/devicecli/grpc"
	"rainiot/pkg/devicecli/httpc"

	"github.com/zeromicro/go-zero/zrpc"
)

type DeviceCli interface {
	Push(ctx context.Context, connId string, message []byte) ([]byte, error)
	// Pull(ctx context.Context) (message []byte, err error)
}

type deviceCli struct {
	model string
	http  DeviceCli
	zrpc  DeviceCli
}

func NewDeviceCli(model, serviceName string, fn func(serviceName string) []string, zrpcConf zrpc.RpcClientConf) DeviceCli {
	return &deviceCli{
		model: model,
		http:  httpc.NewDeviceCli(serviceName, fn),
		zrpc:  grpc.NewDeviceCli(serviceName, zrpcConf),
	}
}

func (d *deviceCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	if d.model == "grpc" {
		return d.zrpc.Push(ctx, connId, message)
	}
	return d.http.Push(ctx, connId, message)
}
