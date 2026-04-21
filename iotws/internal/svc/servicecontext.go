// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"strings"

	"rainiot/iotws/internal/config"

	goredis "github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config     config.Config
	Redis      *goredis.ClusterClient
	Connection *connection
}

func NewServiceContext(c config.Config) *ServiceContext {
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: NewConnection(c),
	}
}
