package grpc

import (
	"context"
	"time"

	"rainiot/pkg/devicecli/pb"

	"github.com/zeromicro/go-zero/zrpc"
	_ "github.com/zeromicro/zero-contrib/zrpc/registry/nacos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type deviceGrpcCli struct {
	client        pb.IotdeviceClient
	serviceName   string
	retryCount    int
	retryInterval time.Duration
}

const (
	defaultDeviceServiceName = "iotdevice_grpc"
	defaultRetryCount        = 3
	defaultRetryInterval     = 1 * time.Second
)

func NewDeviceCli(serviceName string, zrpcConf zrpc.RpcClientConf, fn func(devServiceName string, zrpcConf *zrpc.RpcClientConf)) *deviceGrpcCli {
	fn(defaultDeviceServiceName, &zrpcConf)
	// zrpcConf = zrpc.RpcClientConf{
	// 	Endpoints: []string{"127.0.0.1:9090"},
	// }
	println(zrpcConf.Target)
	conn, err := zrpc.NewClientWithTarget(
		"nacos://iot:root@192.168.9.21:8848/iotdevice_grpc?namespaceid=iot&group=DEFAULT_GROUP",
		zrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		panic(err)
	}
	// conn := zrpc.MustNewClient(zrpcConf)
	return &deviceGrpcCli{
		client:        pb.NewIotdeviceClient(conn.Conn()),
		serviceName:   serviceName,
		retryCount:    defaultRetryCount,
		retryInterval: defaultRetryInterval,
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
	if err == nil {
		return nil, nil
	}
	if !isRetryable(err) {
		return nil, err
	}

	for i := 0; i < d.retryCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.retryInterval):
		}

		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = d.client.DeviceConnect(callCtx, req)
		cancel()
		if err == nil {
			return nil, nil
		}
		if !isRetryable(err) {
			return nil, err
		}
	}
	return nil, err
}

func isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}
