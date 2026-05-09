// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/pkg/openconfig"

	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config     config.Config
	Redis      *redis.ClusterClient
	Connection *connection
}

func NewServiceContext(c config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: NewConnection(c),
	}
}
