// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"crypto/aes"
	"crypto/cipher"

	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/app/iotdevice/cmd/internal/types"
	"rainiot/pkg/cache"
	"rainiot/pkg/client/wscli"
	"rainiot/pkg/errorx"

	"github.com/zeromicro/go-zero/core/logx"
)

type iotLoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func newIotLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotLoginLogic {
	return &iotLoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotLoginLogic) Iotdevice(req types.IotdeviceReq) (resp *types.Response, err error) {
	if req.GetSn() == "" {
		l.Logger.Errorf("device sn cannot be empty")
		return nil, errorx.NewMyError(5000, "device sn cannot be empty")
	}
	// 使用 GET 替代 EXISTS+GET 两次往返：GET 返回 nil 即表示 key 不存在。
	ciphertext := req.GetData().AsMap()["ciphertext"]

	loginCache, onlined, err := l.svcCtx.Cache.GetLoginBySn(l.ctx, req.GetSn())
	if err != nil {
		l.Logger.Errorf("redis get error: %v", err)
		return nil, err
	}
	err = l.loginVerification(loginCache, ciphertext)
	if err != nil {
		l.Logger.Errorf("login verification error: %v", err)
		return nil, err
	}
	device, ok, err := l.svcCtx.DeviceModel.GetOneBySn(l.ctx, req.GetSn())
	if err != nil {
		l.Logger.Errorf("device model get error: %v", err)
		return nil, err
	}
	if !ok {
		l.Logger.Errorf("device not found")
		return nil, errorx.NewMyError(4000, "device not found")
	}
	if ciphertext != nil {
		if !device.Key.Valid {
			l.Logger.Errorf("device key is null")
			return nil, errorx.NewMyError(4000, "device key is null")
		}
		date, err := l.decryptGCM(ciphertext.([]byte), []byte(device.Key.String))
		if err != nil {
			l.Logger.Errorf("decryptGCM error: %v", err)
			return nil, errorx.NewMyError(5000, "decryptGCM error")
		}
		if string(date) != device.Sn {
			return nil, errorx.NewMyError(4000, "device sn not match")
		}
	}
	if onlined {
		offline := wscli.Request{
			ConnId: device.Sn,
		}
		err = l.svcCtx.Wscli.Push(l.ctx, "kill", offline, nil)
		if err != nil {
			l.Logger.Errorf("push offline error: %v", err)
		}
	}
	err = l.svcCtx.Cache.SetLoginBySn(l.ctx, req.GetSn(), cache.Login{
		ServiceName: req.GetServiceName(),
		IsEncrypt:   ciphertext != nil,
	})
	if err != nil {
		l.Logger.Errorf("redis set error: %v", err)
		return nil, err
	}
	return &types.Response{
		Cmd:    req.GetCmd() + "Ack",
		ConnId: req.GetSn(),
	}, nil
}

func (l *iotLoginLogic) loginVerification(loginCache cache.Login, ciphertext any) error {

	if ciphertext != nil {
		return nil
	}
	if !loginCache.IsEncrypt {
		return nil
	}

	return errorx.NewMyError(4000, "device already login")
}

func (l *iotLoginLogic) decryptGCM(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errorx.NewMyError(4001, "ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
