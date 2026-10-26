// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"
	"rainiot/pkg/errors"

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

func (l *iotErrorLogic) Iotdevice(req types.IotdeviceReq) (*types.Response, error) {
	return nil, errors.NewMyError(4001, "unsupported cmd: "+req.GetCmd())
}
