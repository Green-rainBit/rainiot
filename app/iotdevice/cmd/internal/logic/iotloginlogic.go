// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"

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
	if req.Sn == "" {
		return nil, errors.New("device sn cannot be empty")
	}
	exists, err := l.svcCtx.Redis.Exists(l.ctx, "conn:"+req.Sn).Result()
	if err != nil {
		return nil, err
	}
	if exists == 1 {
		return nil, errors.New("device already logged in")
	}

	exists, err = l.svcCtx.Redis.Exists(l.ctx, req.Sn).Result()
	if err != nil {
		l.Logger.Errorf("redis Exists", err)
		return nil, err
	}
	_, ok, err := l.svcCtx.DeviceModel.GetOneBySn(l.ctx, req.Sn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("device not found")
	}
	err = l.svcCtx.Redis.Set(l.ctx, req.Sn, "1", 0).Err()
	if err != nil {
		return nil, err
	}
	return &types.Response{
		Message: "success",
	}, nil
}
