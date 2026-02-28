// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/iotws/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotwsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IotwsLogic {
	return &IotwsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}
