// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package config

import (
	"rainiot/pkg/devicecli/nats"

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
	// Nats NATS 服务端订阅配置，用于消费 NATS 消息。
	Nats nats.NatsConf `json:",optional"`
}
