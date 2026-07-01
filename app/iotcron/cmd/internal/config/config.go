package config

import (
	"rainiot/pkg/log/logloki"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	CacheRedis   redis.RedisConf
	DeviceServer string
	DevicePort   int
	DviceHost    string
	Loki         logloki.LokiConf `json:",optional"`
}
