// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"rainiot/app/iotws/cmd/internal/svc"

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

func (l *IotwsLogic) Iotws(message []byte) (by []byte, err error) {
	resp, err := http.Post(l.svcCtx.Config.DeviceServer, "application/json", bytes.NewReader(message))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("")
	}

	return io.ReadAll(resp.Body)
}
