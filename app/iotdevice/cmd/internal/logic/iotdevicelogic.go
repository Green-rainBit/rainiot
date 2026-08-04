// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/pkg/errors"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"
)

type IotdeviceLogic interface {
	Iotdevice(req types.IotdeviceReq) (resp *types.Response, err error)
}

type iotdeviceLogic struct {
	ctx context.Context
	l   IotdeviceLogic
}

func (l iotdeviceLogic) Iotdevice(req types.IotdeviceReq) (*types.Response, error) {
	resp, err := l.l.Iotdevice(req)
	if err != nil {
		return nil, errors.NewMyErrorWithCtx(l.ctx, err)
	}
	return resp, nil
}
func NewIotdeviceLogic(ctx context.Context, cmd string, svcCtx *svc.ServiceContext) IotdeviceLogic {
	var logic IotdeviceLogic
	switch cmd {
	case "login":
		logic = newIotLoginLogic(ctx, svcCtx)
	default:
		logic = newIotErrorLogic(ctx, svcCtx)
	}
	return iotdeviceLogic{ctx: ctx, l: logic}
}
