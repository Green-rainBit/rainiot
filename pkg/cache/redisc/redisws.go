package redisc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"rainiot/pkg/cache/consts"

	"github.com/redis/go-redis/v9"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	renewConnLua = `
			local t = redis.call("TYPE", KEYS[1])["ok"]
			if t == "none" then return -1 end
			if t ~= "hash" then return 0 end

			local owner = redis.call("HGET", KEYS[1], "serviceName")
			if not owner then return 0 end
			if owner ~= ARGV[1] then return 0 end
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
			`
	// Delete check comfort for this service
	renewConn = `
		local owner = redis.call("HGET", KEYS[1], "serviceName")
		if not owner then return 0 end
		if owner ~= ARGV[1] then return 0 end
		return redis.call("DEL", KEYS[1])
		`
)

type redisCilws struct {
	redis *goredis.ClusterClient
}

func NewCacheCliWsClient(redis *goredis.ClusterClient) *redisCilws {
	return &redisCilws{
		redis: redis,
	}
}

func (r *redisCilws) SetInstanceIdByconnId(ctx context.Context, connId, instanceId string) error {
	if err := goredis.NewScript(renewConn).Run(ctx, r.redis,
		[]string{consts.GetCacheConn(connId)}, instanceId).Err(); err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set instanceId by connId: %v", err))
		return err
	}
	return nil
}

func (r *redisCilws) SetWsBalancedLastTime(ctx context.Context) error {
	err := r.redis.Set(ctx, consts.GetCacheWsBalancedLast(), time.Now().Unix(), 0).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set ws balanced last time: %v", err))
		return err
	}
	return nil
}

func (r *redisCilws) SetConnNumByInstanceId(ctx context.Context, instanceId string, num int64) error {
	err := r.redis.Set(ctx, consts.GetWsBalancedPublishCache(instanceId), num, time.Duration(5)*time.Second).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set conn num by instance id: %v", err))
		return err
	}
	return nil
}

func (r *redisCilws) RenewalWsConnByInstanceId(ctx context.Context, instanceId string, connectionIds []string) ([]*redis.Cmd, error) {
	pipe := r.redis.Pipeline()
	cmds := make([]*goredis.Cmd, len(connectionIds))

	for i, connectionId := range connectionIds {
		cmds[i] = pipe.Eval(ctx, renewConnLua,
			[]string{consts.GetCacheConn(connectionId)}, instanceId,
			consts.ConnTime.Milliseconds())
	}

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, goredis.Nil) {
		logx.WithContext(ctx).Error(fmt.Sprintf("renewal ws conn by instanceId: %v", err))
		return nil, err
	}
	return cmds, nil
}

func (r *redisCilws) SetAddrByInstanceId(ctx context.Context, instanceId, addr string) error {
	err := r.redis.Set(ctx, consts.GetCacheWsServerName(instanceId), addr, time.Duration(6)*time.Minute).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set addr by instanceId: %v", err))
		return err
	}
	return nil
}

func (r *redisCilws) SubscribeTaskByInstanceId(ctx context.Context, instanceId string) <-chan *goredis.Message {
	sub := r.redis.Subscribe(ctx, consts.SubscribeByIotTaskQueueAndInstanceId(instanceId))
	return sub.Channel()
}

func (r *redisCilws) PublishTaskByMsgId(ctx context.Context, msgId string, message any) error {
	err := r.redis.Publish(ctx, consts.SubscribeByIotTaskOKMsgId(msgId), message).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("publish by messgId error: %v", err))
		return err
	}
	return nil
}
