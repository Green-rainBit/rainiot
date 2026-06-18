// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"bytes"
	"context"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/devicecli"

	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsLogic struct {
	logx.Logger
	ctx       context.Context
	deviceCli devicecli.DeviceCli
	svc       *svc.ServiceContext
}

func NewIotwsLogic(ctx context.Context, deviceCli devicecli.DeviceCli, svc *svc.ServiceContext) *IotwsLogic {
	return &IotwsLogic{
		Logger:    logx.WithContext(ctx),
		ctx:       ctx,
		deviceCli: deviceCli,
		svc:       svc,
	}
}

func (l *IotwsLogic) Iotws(message []byte) (by []byte, err error) {
	connId := extractConnId(message)
	resp, err := l.deviceCli.Push(l.ctx, "grpc", connId, message)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func extractConnId(payload []byte) string {
	key := []byte(`"connId":"`)
	i := bytes.Index(payload, key)
	if i == -1 {
		return ""
	}
	start := i + len(key)
	end := bytes.IndexByte(payload[start:], '"')
	if end == -1 {
		return ""
	}
	return string(payload[start : start+end])
}
