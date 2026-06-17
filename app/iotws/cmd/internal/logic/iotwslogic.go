// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/devicecli"

	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsLogic struct {
	logx.Logger
	ctx       context.Context
	deviceCli devicecli.DeviceCli
	svc       *svc.ServiceContext
}

func NewIotwsLogic(ctx context.Context, deviceCli devicecli.DeviceCli, svc *svc.ServiceContext) *IotwsLogic {
	return &IotwsLogic{
		Logger:    logx.WithContext(ctx),
		ctx:       ctx,
		deviceCli: deviceCli,
		svc:       svc,
	}
}

func (l *IotwsLogic) Iotws(message []byte) (by []byte, err error) {

	resp, err := l.deviceCli.Push(l.ctx, "grpc", message)
	if err != nil {
		return nil, err
	}
	return resp, err
}
