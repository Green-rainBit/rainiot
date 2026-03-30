// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"rainiot/iotdevice/internal/config"
	"rainiot/iotdevice/internal/model"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config      config.Config
	DeviceModel model.DeviceModel
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.DataSource)
	return &ServiceContext{
		Config:      c,
		DeviceModel: model.NewDeviceModel(conn, c.CacheRedis),
	}
}
