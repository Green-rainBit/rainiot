// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotAckLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func newIotAckLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotErrorLogic {
	return &iotErrorLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotAckLogic) Iotdevice(req types.IotdeviceReq) (*types.Response, error) {
	msg := req.GetData().AsMap()
	return nil, l.svcCtx.Redis.Publish(l.ctx, req.GetCmd()+msg["messgeId"].(string), req).Err()
}
