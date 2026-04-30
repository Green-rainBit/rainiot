// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"rainiot/iotdevice/internal/config"
	"rainiot/iotdevice/model"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config      config.Config
	DeviceModel model.DeviceModel
	Redis       *goredis.ClusterClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.DataSource)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	return &ServiceContext{
		Config:      c,
		DeviceModel: model.NewDeviceModel(conn),
		Redis:       client,
	}
}
