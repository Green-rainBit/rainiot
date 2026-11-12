package device

import (
	"context"
	"encoding/json"
	"rainiot/pkg/cache"

	goredis "github.com/redis/go-redis/v9"
)

type redisCil struct {
	redis *goredis.ClusterClient
}

func NewCacheDeviceClient(redis *goredis.ClusterClient) cache.CacheCliDevice {
	return &redisCil{
		redis: redis,
	}
}

func (r *redisCil) SetLoginBySn(ctx context.Context, sn string, cacheLogin cache.Login) error {
	return r.redis.Set(ctx, cache.GetCacheConn(sn), cacheLogin, cache.ConnTime).Err()
}

func (r *redisCil) GetLoginBySn(ctx context.Context, sn string) (cache.Login, bool, error) {
	bytes, err := r.redis.Get(ctx, cache.GetCacheConn(sn)).Result()
	if err == goredis.Nil {
		return cache.Login{}, false, nil
	}
	if err != nil {
		return cache.Login{}, false, err
	}
	var login cache.Login
	err = json.Unmarshal([]byte(bytes), &login)
	if err != nil {
		return cache.Login{}, false, err
	}
	return login, true, nil
}

func (r *redisCil) Exists(ctx context.Context, key ...string) (int64, error) {
	return r.redis.Exists(ctx, key...).Result()

}

func (r *redisCil) Publish(ctx context.Context, channel string, message interface{}) error {
	return r.redis.Publish(ctx, channel, message).Err()
}
