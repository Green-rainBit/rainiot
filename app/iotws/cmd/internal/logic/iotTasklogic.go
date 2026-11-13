// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/app/iotws/cmd/internal/svc"

	"github.com/lxzan/gws"
	"github.com/zeromicro/go-zero/core/logx"
)

type iotTaskLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotTaskLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotTaskLogic {
	return &iotTaskLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotTaskLogic) IotTask(event, connId string, msg string) error {
	conn, ok := l.svcCtx.Connection.GetConnByConnId(connId)
	if !ok {
		return errors.New("object not connected")
	}
	switch co := conn.(type) {
	case *gws.Conn:
		return co.WriteMessage(gws.OpcodeText, []byte(msg))
	default:
		// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
	}

	return nil
}
