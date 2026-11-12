package cache

import (
	"context"
	"encoding/json"

	goredis "github.com/redis/go-redis/v9"
)

type Login struct {
	ServiceName string `json:"serviceName"`
	IsEncrypt   bool   `json:"isEncrypt"`
}

func (l Login) MarshalBinary() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Login) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, l)
}

type CacheCliDevice interface {
	SetLoginBySn(ctx context.Context, sn string, cacheLogin Login) error
	GetLoginBySn(ctx context.Context, sn string) (Login, bool, error)
	Exists(ctx context.Context, key ...string) (int64, error)
	Publish(ctx context.Context, channel string, message interface{}) error
}

type CacheCliWs interface {
	Script(ctx context.Context, src, connId, instanceId string) error
	SetWsBalancedLastTime(ctx context.Context) error
	SetConnNumByInstanceId(ctx context.Context, instanceId string, num int64) error
	RenewalWsConnByInstanceId(ctx context.Context, instanceId string, connectionIds []string) ([]*goredis.Cmd, error)
}

// func NewCacheCliDevice(client *redis.Client) CacheCliDevice {
// 	return &cacheCliDevice{client: client}
// }

// func NewCacheCliWs(client *redis.Client) CacheCliWs {
// 	return &ws.NewRedisClient{client: client}
// }
