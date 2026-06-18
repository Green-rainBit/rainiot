package grpc

import (
	"context"
	"rainiot/pkg/devicecli/pb"

	"github.com/zeromicro/go-zero/zrpc"
	_ "github.com/zeromicro/zero-contrib/zrpc/registry/nacos"
	"google.golang.org/protobuf/encoding/protojson"
)

type deviceGrpcCli struct {
	client      pb.IotdeviceClient
	serviceName string
}

func NewDeviceCli(serviceName string, zrpcConf zrpc.RpcClientConf, fn func(serviceName string, zrpcConf *zrpc.RpcClientConf)) *deviceGrpcCli {
	fn(serviceName, &zrpcConf)

	conn := zrpc.MustNewClient(zrpcConf)
	return &deviceGrpcCli{
		client:      pb.NewIotdeviceClient(conn.Conn()),
		serviceName: serviceName,
	}
}

func (d *deviceGrpcCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	req := &pb.DeviceConnectReq{}
	if err := protojson.Unmarshal(message, req); err != nil {
		return nil, err
	}
	req.ConnId = connId
	req.ServiceName = d.serviceName
	_, err := d.client.DeviceConnect(ctx, req)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
