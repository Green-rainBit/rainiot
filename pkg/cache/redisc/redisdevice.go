package redisc

import (
	"context"
	"errors"
	"fmt"
	"rainiot/pkg/cache/consts"
	"rainiot/pkg/cache/ctypes"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
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
	err := r.redis.HSet(ctx, consts.GetCacheConn(sn), cacheLogin).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set login by sn error: %v", err))
		return err
	}
	err = r.redis.Expire(ctx, consts.GetCacheConn(sn), consts.ConnTime).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set login by sn expire error: %v", err))
		return err
	}
	return nil
}

func (r *redisDevice) GetLoginBySn(ctx context.Context, sn string) (ctypes.Login, bool, error) {
	var login ctypes.Login
	err := r.redis.HGetAll(ctx, consts.GetCacheConn(sn)).Scan(&login)
	if errors.Is(err, goredis.Nil) {
		return ctypes.Login{}, false, nil
	}
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("get login by sn error: %v", err))
		return ctypes.Login{}, false, err
	}
	return login, true, nil
}

func (r *redisDevice) GetAddrByInstanceId(ctx context.Context, instanceId string) (string, bool, error) {
	addr, err := r.redis.Get(ctx, consts.GetCacheWsServerName(instanceId)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", false, nil
	}
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("set addr by instanceId: %v", err))
		return "", false, err
	}
	return addr, true, nil
}

func (r *redisDevice) Exists(ctx context.Context, key ...string) (int64, error) {
	return r.redis.Exists(ctx, key...).Result()

}

func (r *redisDevice) PublishByMessgId(ctx context.Context, event, messgId string, message interface{}) error {
	err := r.redis.Publish(ctx, consts.GetIotTaskQueueByEventAndMsgId(event, messgId), message).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("publish by messgId error: %v", err))
		return err
	}
	return nil
}

func (r *redisDevice) PublishMsgByInstanceId(ctx context.Context, instanceId string, message interface{}) error {
	err := r.redis.Publish(ctx, consts.PublishByIotTaskQueueAndInstanceId(instanceId), message).Err()
	if err != nil {
		logx.WithContext(ctx).Error(fmt.Sprintf("publish msg by instanceId error: %v", err))
		return err
	}
	return nil
}

func (r *redisDevice) SubscribeByMessgId(ctx context.Context, msgId string) <-chan *goredis.Message {
	sub := r.redis.Subscribe(ctx, consts.SubscribeByIotTaskOKMsgId(msgId))
	return sub.Channel()
}

func (r *redisDevice) SubscribeByEventMessgId(ctx context.Context, event, messgId string) <-chan *goredis.Message {
	sub := r.redis.Subscribe(ctx, consts.SubscribeIotTaskQueueAckByEventAndMsgId(event, messgId))
	return sub.Channel()
}
