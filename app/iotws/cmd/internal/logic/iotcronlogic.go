// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"fmt"

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
	err := l.svcCtx.Redis.HSet(l.ctx, cache.GetCacheWsConn(), l.svcCtx.Config.Name, l.svcCtx.Connection.GetNumber()).Err()
	if err != nil {
		l.Logger.Error(fmt.Sprintf("hset error: %v", err))
	}
}
