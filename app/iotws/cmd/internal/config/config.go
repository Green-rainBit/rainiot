// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package config

import (
	"sync"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	_ "github.com/zeromicro/zero-contrib/zrpc/registry/nacos"
)

type Config struct {
	rest.RestConf

	RpcClientConf   zrpc.RpcClientConf
	CacheRedis      redis.RedisConf
	DeviceServerMap sync.Map
}

func (c *Config) SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf) {

}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	value, ok := c.DeviceServerMap.Load(serviceName)
	strings, ok := value.([]string)
	if !ok {
		return nil
	}
	return strings

}
