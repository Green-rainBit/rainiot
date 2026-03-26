// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/iotdevice/internal/svc"
	"rainiot/iotdevice/internal/types"
)

type IotdeviceLogic interface {
	Iotdevice(req *types.Request) (resp *types.Response, err error)
}

func NewIotdeviceLogic(ctx context.Context, cmd string, svcCtx *svc.ServiceContext) IotdeviceLogic {
	switch cmd {
	case "login":
		return newIotLoginLogic(ctx, svcCtx)
	default:
		return newIotErrorLogic(ctx, svcCtx)
	}
}
