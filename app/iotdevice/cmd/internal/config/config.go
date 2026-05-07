// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package config

import (
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type NacosConfig struct {
	NacosSeverConfig     []constant.ServerConfig
	NacosAppClientConfig constant.ClientConfig
}

type Config struct {
	rest.RestConf
	DataSource string
	CacheRedis redis.RedisConf
}
