package config

import (
	"time"

	"rainiot/pkg/alarm"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/openconfig"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type Config struct {
	rest.RestConf

	CacheRedis redis.RedisConf

	TransportModel string `json:""`
	DeviCliConfig  openconfig.DeviCliConfig
	Loki           logloki.LokiConf `json:""`

	AlarmConfig alarm.Config `json:""`

	WsConf WsConf `json:""`
}

// WsConf WebSocket 服务器配置。
type WsConf struct {
	PingInterval        time.Duration `json:",default=25s"`
	PingWait            time.Duration `json:",default=75s"`
	ReadBufferSize      int           `json:",default=8192"`
	WriteBufferSize     int           `json:",default=8192"`
	ReadMaxPayloadSize  int           `json:",default=8192"`
	WriteMaxPayloadSize int           `json:",default=8192"`
	ParallelEnabled     bool          `json:",default=true"`
	ParallelGolimit     int           `json:",default=1024"`
	CheckUtf8Enabled    bool          `json:",default=false"`
}
