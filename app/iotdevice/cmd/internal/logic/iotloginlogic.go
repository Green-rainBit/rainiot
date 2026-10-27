// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	errorse "errors"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"
	"rainiot/pkg/cache"
	"rainiot/pkg/errors"

	"github.com/redis/go-redis/v9"
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
	// 使用 GET 替代 EXISTS+GET 两次往返：GET 返回 nil 即表示 key 不存在。
	cur, err := l.svcCtx.Redis.Get(l.ctx, cache.GetCacheConn(req.GetSn())).Result()
	switch {
	case errorse.Is(err, redis.Nil):
		// 未连接，继续
	case err != nil:
		return nil, err
	case cur == req.GetServiceName():
		// 同实例重复登录：刷新 TTL（幂等）
	default:
		// 其他实例持有：拒绝或接管，二选一
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
