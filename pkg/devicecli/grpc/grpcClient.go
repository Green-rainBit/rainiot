package grpc

import (
	"context"
	"time"

	"rainiot/pkg/devicecli/grpc/pb"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/codes"
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
	DefaultDeviceServiceName = "iotdevice.grpc"
	defaultRetryCount        = 3
	defaultRetryInterval     = 1 * time.Second
)

func NewDeviceCli(serviceName string, zrpcConf zrpc.RpcClientConf) *deviceGrpcCli {
	conn := zrpc.MustNewClient(zrpcConf)
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

func (d *deviceGrpcCli) Information() string {
	return "device grpc client, serviceName: " + d.serviceName
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

func (d *deviceGrpcCli) Close() error {
	return nil
}
