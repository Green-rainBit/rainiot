// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type IotPingLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotPingLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IotPingLogic {
	return &IotPingLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IotPingLogic) IotPing(req *types.Request) (err error) {
	_, ok := l.svcCtx.Connection.GetconnByConnId(req.ConnId)
	if !ok {
		return errors.New("设备未连接")
	}
	return
}
