// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"encoding/json"
	"time"

	"rainiot/app/iotcron/cmd/internal/svc"
	"rainiot/pkg/cache/consts"
	"rainiot/pkg/cache/ctypes"

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
	timeUnix, err := l.svcCtx.Cache.GetCacheWsBalancedLast(l.ctx)
	if err == redis.Nil {
		// 首次运行，初始化时间戳后继续
		_ = l.svcCtx.Cache.SetWsBalancedLastTime(l.ctx)
	} else if err != nil {
		return
	}
	timeLastat := time.Unix(timeUnix, 0)
	if timeLastat.Add(time.Minute * time.Duration(5)).After(time.Now()) {
		return
	}
	allKeys, err := l.svcCtx.Cache.GetAllWsBalancedPublishCache(l.ctx)
	if err != nil {
		return
	}
	wsBalancedMap := make(map[string]int, len(allKeys))
	totalws := 0
	wsBalancedMap, err = l.svcCtx.Cache.GetDelConnNumByKeys(l.ctx, allKeys)
	if err != nil {
		return
	}
	for _, num := range wsBalancedMap {
		totalws += num
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
		pub := ctypes.WsBalancedPublish{
			Meanws:                int64(meanws),
			ReceiveWsserver:       val.ReceiveWsserver,
			ReceiveWsserverAmount: val.ReceiveWsserverAmount,
		}
		body, err := json.Marshal(pub)
		if err != nil {
			// 不要 return，否则后面的目标实例都不会收到任务
			l.Logger.Errorf("marshal balance plan failed target=%s: %v", disconnectWsServerName, err)
			continue
		}

		queue := consts.WsBalanceQueue(disconnectWsServerName)
		task := asynq.NewTask(consts.TaskTypeWsBalance, body)

		info, err := l.svcCtx.AsynqClient.Enqueue(task,
			asynq.Queue(queue),
			asynq.MaxRetry(3),
			asynq.Timeout(30*time.Second),
		)
		if err != nil {
			l.Logger.Errorf("enqueue balance task failed target=%s queue=%s: %v",
				disconnectWsServerName, queue, err)
			continue
		}

		l.Logger.Infof("balance task enqueued target=%s queue=%s taskId=%s targets=%d",
			disconnectWsServerName, queue, info.ID, len(val.ReceiveWsserver))
	}

}
