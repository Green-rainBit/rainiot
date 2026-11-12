package redisc

import (
	"context"
	"fmt"
	"rainiot/pkg/cache/consts"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type redisCron struct {
	redis *goredis.ClusterClient
}

func NewCacheCronClient(redis *goredis.ClusterClient) *redisCron {
	return &redisCron{
		redis: redis,
	}
}

func (r *redisCron) GetCacheWsBalancedLast(ctx context.Context) (int64, error) {
	return r.redis.Get(ctx, consts.GetCacheWsBalancedLast()).Int64()
}

func (r *redisCron) SetWsBalancedLastTime(ctx context.Context) error {
	err := r.redis.Set(ctx, consts.GetCacheWsBalancedLast(), time.Now().Add(-5*time.Minute).Unix(), 0).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set ws balanced last time: %v", err))
		return err
	}
	return nil
}

func (r *redisCron) GetAllWsBalancedPublishCache(ctx context.Context) ([]string, error) {
	var allKeys []string
	pattern := consts.GetWsBalancedPublishCache("") + "*"

	err := r.redis.ForEachMaster(ctx, func(ctx context.Context, rdb *redis.Client) error {
		iter := rdb.Scan(ctx, 0, pattern, 100).Iterator()
		for iter.Next(ctx) {
			allKeys = append(allKeys, iter.Val())
		}
		return iter.Err()
	})
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("get all ws balanced publis cache: %v", err))
		return nil, err
	}
	return allKeys, nil
}

func (r *redisCron) GetDelConnNumByKeys(ctx context.Context, allKeys []string) (map[string]int, error) {
	pipe := r.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(allKeys))
	for i, k := range allKeys {
		cmds[i] = pipe.GetDel(ctx, k)
	}

	// Exec 返回的 err 可能是 redis.Nil（部分 key 不存在），不是致命错误
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("pipe exec error: %v", err))
		return nil, err
	}
	res := map[string]int{}
	prefix := consts.GetWsBalancedPublishCache("")
	for i, cmd := range cmds {
		instanceId := strings.TrimPrefix(allKeys[i], prefix)
		if instanceId == "" {
			continue
		}
		v, err := cmd.Int()
		if err == redis.Nil {
			res[instanceId] = 0
			continue
		}
		if err != nil {
			// 单条命令错误（类型错误、网络等）也应保留位置
			res[instanceId] = 0
			logx.WithContext(ctx).Error(fmt.Sprintf("getdel error: %v", err))
			continue
		}
		res[instanceId] = v
	}
	return res, nil
}

func (r *redisCron) Close() error {
	return r.redis.Close()
}
