// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"fmt"

	"rainiot/app/iotws/cmd/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotsyncAddrLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewiotsyncAddrLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotsyncAddrLogic {
	return &iotsyncAddrLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotsyncAddrLogic) Iotsync() {
	err := l.svcCtx.Cache.SetAddrByInstanceId(l.ctx, l.svcCtx.GetInstanceId(), l.svcCtx.Config.NotifyAddr)
	if err != nil {
		l.Logger.Error(fmt.Sprintf("iot sync addr logic iot sync setxx error: %v", err))
	}
}
