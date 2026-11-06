package logic

import (
	"context"
	"errors"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"

	"github.com/lxzan/gws"
	"github.com/zeromicro/go-zero/core/logx"
)

type iotKillLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotKillLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotKillLogic {
	return &iotKillLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotKillLogic) IotKill(req *types.Notice) error {
	conn, ok := l.svcCtx.Connection.GetConnByConnId(req.ConnId)
	if !ok {
		return errors.New("设备未连接")
	}
	switch co := conn.(type) {
	case *gws.Conn:
		l.svcCtx.CleanupSocket(co)
	default:
		// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
	}
	return nil
}
