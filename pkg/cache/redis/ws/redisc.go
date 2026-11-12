package ws

import (
	"context"
	"errors"
	"rainiot/pkg/cache"
	"time"

	"github.com/redis/go-redis/v9"
	goredis "github.com/redis/go-redis/v9"
)

type redisCil struct {
	redis *goredis.ClusterClient
}

func NewCacheCliWsClient(redis *goredis.ClusterClient) cache.CacheCliWs {
	return &redisCil{
		redis: redis,
	}
}

func (r *redisCil) Script(ctx context.Context, src, connId, instanceId string) error {
	if err := goredis.NewScript(src).Run(ctx, r.redis,
		[]string{cache.GetCacheConn(connId)}, instanceId).Err(); err != nil {
		return err
	}
	return nil
}

func (r *redisCil) SetWsBalancedLastTime(ctx context.Context) error {
	err := r.redis.Set(ctx, cache.GetCacheWsBalancedLast(), time.Now().Unix(), 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *redisCil) SetConnNumByInstanceId(ctx context.Context, instanceId string, num int64) error {
	err := r.redis.Set(ctx, cache.GetWsBalancedPublishCache(instanceId), num, time.Duration(5)*time.Second).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *redisCil) RenewalWsConnByInstanceId(ctx context.Context, instanceId string, connectionIds []string) ([]*redis.Cmd, error) {
	pipe := r.redis.Pipeline()
	cmds := make([]*redis.Cmd, len(connectionIds))

	const renewConnLua = `
			local v = redis.call("GET", KEYS[1])
			if not v then return -1 end
			local owner = v
			local ok, login = pcall(cjson.decode, v)
			if ok and type(login) == "table" then
				owner = login.serviceName or ""
			end
			if owner ~= ARGV[1] then return 0 end
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
			`

	for i, connectionId := range connectionIds {
		cmds[i] = pipe.Eval(ctx, renewConnLua,
			[]string{cache.GetCacheConn(connectionId)}, instanceId,
			cache.ConnTime.Milliseconds())
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		// Pipeline 本身出现的致命错误（网络等）
		return nil, err
	}
	return cmds, nil
}
