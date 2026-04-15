// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"encoding/json"

	"rainiot/iotdevice/internal/svc"
	"rainiot/iotdevice/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotLoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func newIotLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotLoginLogic {
	return &iotLoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotLoginLogic) Iotdevice(req *types.Request) (resp *types.Response, err error) {
	// todo: add your logic here and delete this line
	deviceLoginReq := &types.DeviceLogin{}
	err = json.Unmarshal(req.Data, &deviceLoginReq)
	if err != nil {
		return nil, err
	}
	_, err = l.svcCtx.DeviceModel.FindOneBySn(l.ctx, deviceLoginReq.Sn)
	if err != nil {
		return nil, err
	}

	l.svcCtx.Redis.Set(l.ctx, deviceLoginReq.Sn, "1", 0)
	
	return &types.Response{
		Message: "success",
	}, nil
}
