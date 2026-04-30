// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"encoding/json"
	"errors"

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
	// Keep a concrete struct literal here so gopls can offer fillStruct on model.Device{}.
	deviceLoginReq := &types.DeviceLogin{}
	err = json.Unmarshal(req.Data, &deviceLoginReq)
	if err != nil {
		return nil, err
	}
	if deviceLoginReq.Sn == "" {
		return nil, errors.New("device sn cannot be empty")
	}

	exists, err := l.svcCtx.Redis.Exists(l.ctx, "conn:"+deviceLoginReq.Sn).Result()
	if err != nil {
		return nil, err
	}
	if exists == 1 {
		return nil, errors.New("device already logged in")
	}

	exists, err = l.svcCtx.Redis.Exists(l.ctx, deviceLoginReq.Sn).Result()
	if err != nil {
		return nil, err
	}
	_, ok, err := l.svcCtx.DeviceModel.GetOneBySn(l.ctx, deviceLoginReq.Sn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("device not found")
	}
	err = l.svcCtx.Redis.Set(l.ctx, deviceLoginReq.Sn, "1", 0).Err()
	if err != nil {
		return nil, err
	}
	return &types.Response{
		Message: "success",
	}, nil
}
