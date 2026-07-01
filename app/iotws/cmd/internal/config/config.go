package config

import (
	"sync"

	"rainiot/pkg/alarm"
	"rainiot/pkg/devicecli/nats"
	"rainiot/pkg/log/logloki"

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
	Nats            nats.NatsConf    `json:",optional"`
	Loki            logloki.LokiConf `json:",optional"`
	TransportModel  string           `json:",optional"`
	AlarmConfig     alarm.Config     `json:",optional"`
}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	value, ok := c.DeviceServerMap.Load(serviceName)
	strings, ok := value.([]string)
	if !ok {
		return nil
	}
	return strings
}
