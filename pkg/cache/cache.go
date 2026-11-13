package cache

import (
	"context"

	"rainiot/pkg/cache/ctypes"
	"rainiot/pkg/cache/redisc"

	goredis "github.com/redis/go-redis/v9"
)

type CacheCliDevice interface {
	SetLoginBySn(ctx context.Context, sn string, cacheLogin ctypes.Login) error
	GetLoginBySn(ctx context.Context, sn string) (ctypes.Login, bool, error)
	GetAddrByInstanceId(ctx context.Context, instanceId string) (string, bool, error)
	Exists(ctx context.Context, key ...string) (int64, error)
	PublishByMessgId(ctx context.Context, event, messgId string, message interface{}) error
	SubscribeByEventMessgId(ctx context.Context, event, messgId string) <-chan *goredis.Message
	SubscribeByMessgId(ctx context.Context, msgId string) <-chan *goredis.Message
	PublishMsgByInstanceId(ctx context.Context, instanceId string, message interface{}) error
}

type CacheCliWs interface {
	SetInstanceIdByconnId(ctx context.Context, connId, instanceId string) error
	SetWsBalancedLastTime(ctx context.Context) error
	SetConnNumByInstanceId(ctx context.Context, instanceId string, num int64) error
	RenewalWsConnByInstanceId(ctx context.Context, instanceId string, connectionIds []string) ([]*goredis.Cmd, error)
	SetAddrByInstanceId(ctx context.Context, instanceId, addr string) error
	SubscribeTaskByInstanceId(ctx context.Context, instanceId string) <-chan *goredis.Message
	PublishTaskByMsgId(ctx context.Context, msgId string, message any) error
}

type CacheCliCron interface {
	GetCacheWsBalancedLast(ctx context.Context) (int64, error)
	SetWsBalancedLastTime(ctx context.Context) error
	GetAllWsBalancedPublishCache(ctx context.Context) ([]string, error)
	GetDelConnNumByKeys(ctx context.Context, allKeys []string) (map[string]int, error)
	Close() error
}

func NewCacheCliDevice(client *goredis.ClusterClient) CacheCliDevice {
	return redisc.NewCacheDeviceClient(client)
}

func NewCacheCliWs(client *goredis.ClusterClient) CacheCliWs {
	return redisc.NewCacheCliWsClient(client)
}

func NewCacheCliCron(client *goredis.ClusterClient) CacheCliCron {
	return redisc.NewCacheCronClient(client)
}
