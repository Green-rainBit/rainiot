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
	key := cache.GetWsBalancedPublishCache(l.svcCtx.GetInstanceId())
	value := l.svcCtx.Connection.GetConnByCount()
	expiration := time.Duration(5) * time.Second
	err := l.svcCtx.Redis.Set(l.ctx, key, value, expiration).Err()
	if err != nil {
		l.Logger.Error(fmt.Sprintf("iotsyncLogic iotsync setxx error: %v", err))
	}
}
