// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"rainiot/app/iotcron/cmd/internal/svc"
	"rainiot/pkg/cache"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type IotwsBalancedLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotwsBalancedLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IotwsBalancedLogic {
	return &IotwsBalancedLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *IotwsBalancedLogic) TryBalance(reason string) {
	timeUnix, err := l.svcCtx.Redis.Get(l.ctx, cache.GetCacheWsBalancedLast()).Int64()
	if err == redis.Nil {
		// 首次运行，初始化时间戳后继续
		_ = l.svcCtx.Redis.Set(l.ctx, cache.GetCacheWsBalancedLast(),
			time.Now().Add(-5*time.Minute).Unix(), 0).Err()
	} else if err != nil {
		return
	} 
	timeLastat := time.Unix(timeUnix, 0)
	if timeLastat.Add(time.Minute * time.Duration(5)).After(time.Now()) {
		return
	}
	var allKeys []string
	pattern := cache.GetWsBalancedPublishCache("")

	err = l.svcCtx.Redis.ForEachMaster(l.ctx, func(ctx context.Context, rdb *redis.Client) error {
		iter := rdb.Scan(ctx, 0, pattern, 100).Iterator()
		for iter.Next(ctx) {
			allKeys = append(allKeys, iter.Val())
		}
		return iter.Err()
	})
	if err != nil {
		return
	}
	wsBalancedMap := make(map[string]int, len(allKeys))
	totalws := 0
	prefix := cache.GetWsBalancedPublishCache("")
	for _, key := range allKeys {
		serverName := strings.TrimPrefix(key, prefix)
		if serverName == "" {
			continue
		}
		wsBalancedMap[serverName], err = l.svcCtx.Redis.GetDel(l.ctx, key).Int()
		if err != nil {
			l.Logger.Error(fmt.Sprintf("getdel error: %v", err))
			continue
		}
		totalws += wsBalancedMap[serverName]
	}
	l.IotwsBalanced(wsBalancedMap, totalws)
}

func (l *IotwsBalancedLogic) IotwsBalanced(wsBalancedMap map[string]int, totalws int) {
	if len(wsBalancedMap) == 0 {
		return
	}
	meanws := totalws / len(wsBalancedMap)
	disconnectWsserver := map[string]int{}
	receiveWsserver := map[string]int{}
	offset := 0
	if meanws/5 > 20 {
		offset = meanws / 5
	} else {
		return
	}
	// balanced ws in 20% range fluctuation
	for serverName, totalws := range wsBalancedMap {
		if totalws < meanws-offset {
			receiveWsserver[serverName] = meanws - totalws
		}
		if totalws > meanws+offset {
			disconnectWsserver[serverName] = totalws - meanws
		}
	}
	disconnectWsOutreceiveSeverMap := map[string]struct {
		ReceiveWsserver       []string
		ReceiveWsserverAmount []int64
	}{}
	for serverName, totalws := range disconnectWsserver {
		receiveWsserverNames := []string{}
		receiveWsserverAmount := []int64{}
		for receiveWsserverName, receiveWs := range receiveWsserver {
			if receiveWs == 0 {
				continue
			}
			if receiveWs < totalws {
				totalws -= receiveWs
				receiveWsserverAmount = append(receiveWsserverAmount, int64(receiveWs))
				receiveWsserver[receiveWsserverName] = 0
			} else {
				receiveWsserver[receiveWsserverName] = receiveWs - totalws
				receiveWsserverAmount = append(receiveWsserverAmount, int64(totalws))
			}
			receiveWsserverNames = append(receiveWsserverNames, receiveWsserverName)
		}
		disconnectWsOutreceiveSeverMap[serverName] = struct {
			ReceiveWsserver       []string
			ReceiveWsserverAmount []int64
		}{
			ReceiveWsserver:       receiveWsserverNames,
			ReceiveWsserverAmount: receiveWsserverAmount,
		}
	}

	for disconnectWsServerName, val := range disconnectWsOutreceiveSeverMap {
		pub := cache.WsBalancedPublish{
			Meanws:                int64(meanws),
			ReceiveWsserver:       val.ReceiveWsserver,
			ReceiveWsserverAmount: val.ReceiveWsserverAmount,
		}
		body, err := json.Marshal(pub)
		if err != nil {
			return
		}
		task := asynq.NewTask(cache.GetWsBalancedPublishCache(disconnectWsServerName), body)
		if _, err := l.svcCtx.AsynqClient.Enqueue(task); err != nil {
			l.Logger.Errorf("enqueue balance task failed: %v", err)
		}

	}

}
