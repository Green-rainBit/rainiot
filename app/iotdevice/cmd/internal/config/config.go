package config

import (
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/openconfig"
	"sync"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Rpc             zrpc.RpcServerConf
	DriverName      string
	DataSource      string
	CacheRedis      redis.RedisConf
	MQ              openconfig.MQConfig `json:"mq"`
	Loki            logloki.LokiConf    `json:",optional"`
	DeviceServerMap map[string][]string `json:"deviceServerMap"`

	mu sync.RWMutex
}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.DeviceServerMap[serviceName]
}
