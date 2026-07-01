package devicecli

import (
	"context"
	"log"

	conf_cli "rainiot/pkg/configcli"
	"rainiot/pkg/devicecli/httpc"
	"rainiot/pkg/devicecli/nats"

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
	nats  DeviceCli
}

func NewDeviceCli(model, serviceName string, confCli conf_cli.ConfigCli, zrpcConf zrpc.RpcClientConf, natsConf *nats.NatsConf) DeviceCli {
	d := &deviceCli{
		model: model,
		http:  httpc.NewDeviceCli(serviceName, confCli.GetHealthyInstances),
		//	zrpc:  grpc.NewDeviceCli(serviceName, zrpcConf),
	}

	if natsConf != nil {
		natsCli, err := nats.NewDeviceCli(serviceName, *natsConf)
		if err != nil {
			if model == "nats" {
				log.Fatalf("init nats client err: %v", err)
			}
			log.Printf("init nats client err (fallback to default): %v", err)
		} else {
			d.nats = natsCli
		}
	}

	return d
}

func (d *deviceCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	switch d.model {
	case "grpc":
		return d.zrpc.Push(ctx, connId, message)
	case "nats":
		return d.nats.Push(ctx, connId, message)
	default:
		return d.http.Push(ctx, connId, message)
	}
}
