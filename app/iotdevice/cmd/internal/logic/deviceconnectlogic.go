package logic

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"

	"rainiot/pkg/devicecli/pb"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeviceConnectLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeviceConnectLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeviceConnectLogic {
	return &DeviceConnectLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 定义一个 DeviceConnect 一元 rpc 方法，请求体和响应体必填。
func (l *DeviceConnectLogic) DeviceConnect(in *pb.DeviceConnectReq) (*pb.DeviceConnectResp, error) {
	// todo: add your logic here and delete this line

	return &pb.DeviceConnectResp{}, nil
}
