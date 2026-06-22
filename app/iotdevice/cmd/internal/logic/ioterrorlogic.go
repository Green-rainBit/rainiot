// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotErrorLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func newIotErrorLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotErrorLogic {
	return &iotErrorLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotErrorLogic) Iotdevice(req types.IotdeviceReq) (resp *types.Response, err error) {
	// todo: add your logic here and delete this line
	resp = &types.Response{
		Message: "error",
	}
	return resp, nil
}
