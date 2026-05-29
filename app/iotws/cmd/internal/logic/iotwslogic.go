// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"
	"io"
	"net/http"

	"rainiot/pkg/devicecli"

	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsLogic struct {
	logx.Logger
	ctx       context.Context
	deviceCli devicecli.DeviceCli
}

func NewIotwsLogic(ctx context.Context, deviceCli devicecli.DeviceCli) *IotwsLogic {
	return &IotwsLogic{
		Logger:    logx.WithContext(ctx),
		ctx:       ctx,
		deviceCli: deviceCli,
	}
}

func (l *IotwsLogic) Iotws(message []byte) (by []byte, err error) {
	resp, err := l.deviceCli.Push(l.ctx, "http", message)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyByte, _ := io.ReadAll(resp.Body)
		return nil, errors.New(string(bodyByte))
	}

	return io.ReadAll(resp.Body)
}
