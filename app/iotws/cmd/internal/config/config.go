// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package config

import (
	"sync"

	"rainiot/pkg/devicecli/nats"
	"rainiot/pkg/log/logloki"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf

	// Rpc gRPC 客户端配置。
	// Model 可选值：
	//   "nacos"     — 使用 Nacos 服务发现（需配合 nacos 配置文件 model=nacos）
	//   "instances" — 轮询 DeviceServerMap 获取实例（10s 间隔），支持热更新
	//   空 / 其他   — 不注册自定义 resolver，走 go-zero 默认直连或 etcd
	Rpc struct {
		zrpc.RpcClientConf
		Model string `json:",optional"`
	} `json:",optional"`
	CacheRedis redis.RedisConf
	// DeviceServerMap 服务实例映射，key 为服务名（如 "iotdevice_api"），value 为地址列表，配置热更新时自动刷新。
	// 作为 "instances" 模式的数据源。
	DeviceServerMap sync.Map
	// Nats NATS 客户端配置，用于 "nats" 数据传输模式。
	Nats nats.NatsConf `json:",optional"`
	Loki logloki.Lokiconfig
}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	value, ok := c.DeviceServerMap.Load(serviceName)
	strings, ok := value.([]string)
	if !ok {
		return nil
	}
	return strings
}
