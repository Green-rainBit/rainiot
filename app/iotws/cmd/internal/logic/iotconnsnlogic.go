// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"
	"rainiot/pkg/util"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotconnsnlogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotconnsnlogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotconnsnlogic {
	return &iotconnsnlogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotconnsnlogic) Iotsync() {
	serviceName, _, _ := util.GetRegistryParameters(l.svcCtx.Config.RestConf)
	l.svcCtx.Connection.Range(func(key, value interface{}) bool {
		switch v := key.(type) {
		case string:
			err := l.svcCtx.Redis.Set(l.ctx, cache.GetCacheConn(v), serviceName, 0).Err()
			if err != nil {
				return true
			}
		}
		return true
	})
}
