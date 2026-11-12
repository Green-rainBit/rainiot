package svc

import (
	"rainiot/app/iotcron/cmd/internal/config"
	"rainiot/pkg/cache"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config config.Config

	AsynqClient *asynq.Client
	Cache       cache.CacheCliCron
}

func NewServiceContext(c config.Config) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	return &ServiceContext{
		Config:      c,
		Cache:       cache.NewCacheCliCron(client),
		AsynqClient: asynq.NewClientFromRedisClient(client),
	}
}
