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

func (l *IotwsLogic) Iotws(connId string, message []byte) (by []byte, ok bool, err error) {
	resp, err := l.deviceCli.Push(l.ctx, connId, message)
	if err != nil {
		return nil, true, err
	}
	return resp, true, err
}
