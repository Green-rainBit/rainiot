package redisc

import (
	"context"
	"encoding/json"
	"rainiot/pkg/cache/consts"
	"rainiot/pkg/cache/ctypes"

	goredis "github.com/redis/go-redis/v9"
)

type redisDevice struct {
	redis *goredis.ClusterClient
}

func NewCacheDeviceClient(redis *goredis.ClusterClient) *redisDevice {
	return &redisDevice{
		redis: redis,
	}
}

func (r *redisDevice) SetLoginBySn(ctx context.Context, sn string, cacheLogin ctypes.Login) error {
	return r.redis.Set(ctx, consts.GetCacheConn(sn), cacheLogin, consts.ConnTime).Err()
}

func (r *redisDevice) GetLoginBySn(ctx context.Context, sn string) (ctypes.Login, bool, error) {
	bytes, err := r.redis.Get(ctx, consts.GetCacheConn(sn)).Result()
	if err == goredis.Nil {
		return ctypes.Login{}, false, nil
	}
	if err != nil {
		return ctypes.Login{}, false, err
	}
	var login ctypes.Login
	err = json.Unmarshal([]byte(bytes), &login)
	if err != nil {
		return ctypes.Login{}, false, err
	}
	return login, true, nil
}

func (r *redisDevice) Exists(ctx context.Context, key ...string) (int64, error) {
	return r.redis.Exists(ctx, key...).Result()

}

func (r *redisDevice) Publish(ctx context.Context, channel string, message interface{}) error {
	return r.redis.Publish(ctx, channel, message).Err()
}
