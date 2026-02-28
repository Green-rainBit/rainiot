// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"rainiot/iotws/internal/config"

	"github.com/lxzan/gws"
)

type ServiceContext struct {
	Config   config.Config
	Upgrader *gws.Upgrader
}

func NewServiceContext(c config.Config, eventHandler gws.Event) *ServiceContext {
	upgrader := gws.NewUpgrader(eventHandler, &gws.ServerOption{
		ParallelEnabled:   true,                                 // 开启并行消息处理
		Recovery:          gws.Recovery,                         // 开启异常恢复
		PermessageDeflate: gws.PermessageDeflate{Enabled: true}, // 开启压缩
	})
	return &ServiceContext{
		Config:   c,
		Upgrader: upgrader,
	}
}
