// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"
	"rainiot/pkg/cache"
	"rainiot/pkg/errors"

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

func (l *iotLoginLogic) Iotdevice(req types.IotdeviceReq) (resp *types.Response, err error) {
	if req.GetSn() == "" {
		l.Logger.Errorf("device sn cannot be empty")
		return nil, errors.NewMyError(5000, "device sn cannot be empty")
	}
	exists, err := l.svcCtx.Redis.Exists(l.ctx, cache.GetCacheConn(req.GetSn())).Result()
	if err != nil {
		return nil, err
	}
	if exists == 1 {
		l.Logger.Errorf("device already logged in")
		return nil, errors.NewMyError(4000, "device already logged in")
	}
	_, ok, err := l.svcCtx.DeviceModel.GetOneBySn(l.ctx, req.GetSn())
	if err != nil {
		return nil, err
	}
	if !ok {
		l.Logger.Errorf("device not found")
		return nil, errors.NewMyError(4000, "device not found")
	}
	err = l.svcCtx.Redis.Set(l.ctx, cache.GetCacheConn(req.GetSn()), req.GetServiceName(), cache.ConnTime).Err()
	if err != nil {
		return nil, err
	}
	return &types.Response{
		ConnId: req.GetSn(),
	}, nil
}
