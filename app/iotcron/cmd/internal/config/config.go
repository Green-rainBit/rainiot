package config

import (
	plog "rainiot/pkg/log"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	CacheRedis   redis.RedisConf
	DeviceServer string
	DevicePort   int
	DviceHost    string
	Log          plog.LogConf `json:",optional"`
}