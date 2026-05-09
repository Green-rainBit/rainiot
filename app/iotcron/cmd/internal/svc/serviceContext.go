package svc

import (
	"rainiot/app/iotcron/cmd/internal/config"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config      config.Config
	AsynqServer *asynq.Server
	Redis       *redis.ClusterClient
	// MiniProgram *miniprogram.MiniProgram

	// OrderRpc      order.Order
	// UsercenterRpc usercenter.Usercenter
}

func NewServiceContext(c config.Config) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	return &ServiceContext{
		Config: c,
		Redis:  client,
	}
}
