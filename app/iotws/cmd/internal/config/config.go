package config

import (
	"sync"

	"rainiot/pkg/alarm"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/openconfig"

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
	DeviceServerMap map[string][]string `json:"DeviceServerMap,optional"`
	Loki            logloki.LokiConf    `json:",optional"`
	TransportModel  string              `json:",optional"`
	AlarmConfig     alarm.Config        `json:",optional"`
	MQ              openconfig.MQConfig `json:"mq,optional"`
	WsConf          WsConf              `json:",optional"`

	mu sync.RWMutex
}

// WsConf WebSocket 服务器配置。
type WsConf struct {
	ReadBufferSize     int  `json:",default=8192"`
	WriteBufferSize    int  `json:",default=8192"`
	ReadMaxPayloadSize  int  `json:",default=8192"`
	WriteMaxPayloadSize int  `json:",default=8192"`
	ParallelEnabled    bool `json:",default=true"`
	ParallelGolimit    int  `json:",default=1024"`
	CheckUtf8Enabled   bool `json:",default=false"`
}

func (c *Config) GetHealthyInstances(serviceName string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.DeviceServerMap[serviceName]
}

// Lock 在热更新写入前加锁，返回解锁函数
func (c *Config) Lock() func() {
	c.mu.Lock()
	return c.mu.Unlock
}
