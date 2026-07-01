package config

import (
	"rainiot/pkg/devicecli/nats"
	plog "rainiot/pkg/log"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Rpc        zrpc.RpcServerConf
	DriverName string
	DataSource string
	CacheRedis redis.RedisConf
	Nats       nats.NatsConf `json:",optional"`
	Log        plog.LogConf  `json:",optional"`
}