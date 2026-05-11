// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"fmt"
	"time"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotsyncLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotsyncLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotsyncLogic {
	return &iotsyncLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotsyncLogic) Iotsync() {
	key := cache.CacheWsServerNameLock(l.svcCtx.Config.Name)
	value := l.svcCtx.Connection.GetNumber()
	expiration := time.Duration(5) * time.Second

	// err := cache.Lock(l.svcCtx.Redis, l.ctx, cache.CacheWsServerNameLock(l.svcCtx.Config.Name))
	// if err != nil {
	// 	return
	// }
	// defer cache.Unlock(l.svcCtx.Redis, l.ctx, cache.CacheWsServerNameLock(l.svcCtx.Config.Name))

	err := l.svcCtx.Redis.SetXX(l.ctx, key, value, expiration).Err()
	if err != nil {
		l.Logger.Error(fmt.Sprintf("iotsyncLogic iotsync setxx error: %v", err))
	}
}
