// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"

	"github.com/lxzan/gws"
	"github.com/zeromicro/go-zero/core/logx"
)

type IotNoticeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotNoticeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IotNoticeLogic {
	return &IotNoticeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IotNoticeLogic) IotNotice(req *types.Ntice) error {
	conn, ok := l.svcCtx.Connection.GetconnByConnId(req.Id)
	if !ok {
		return errors.New("设备未连接")
	}
	switch co := conn.(type) {
	case *gws.Conn:
		return co.WriteMessage(gws.OpcodeText, req.Data)
	default:
		// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
	}
	return nil

}
