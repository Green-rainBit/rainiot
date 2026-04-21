// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/iotws/internal/svc"

	"github.com/lxzan/gws"
	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsNoticeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotwsNoticeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IotwsLogic {
	return &IotwsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IotwsNoticeLogic) IotNotice(sn string, message []byte) (err error) {
	conn, ok := l.svcCtx.Connection.Get(sn)
	if !ok {
		return errors.New("设备未连接")
	}
	switch co := conn.(type) {
	case *gws.Conn:
		return co.WriteMessage(gws.OpcodeText, message)
	default:
		// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
	}
	return nil

}
