package config

import (
	"sync"

	"rainiot/pkg/devicecli/nats"
	plog "rainiot/pkg/log"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Rpc struct {
		zrpc.RpcClientConf
		Model string `json:",optional"`
	} `json:",optional"`
	CacheRedis      redis.RedisConf
	DeviceServerMap sync.Map
	Nats            nats.NatsConf `json:",optional"`
	Log             plog.LogConf  `json:",optional"`
}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	value, ok := c.DeviceServerMap.Load(serviceName)
	strings, ok := value.([]string)
	if !ok {
		return nil
	}
	return strings
}