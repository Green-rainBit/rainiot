// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"fmt"
	"strconv"

	"rainiot/app/iotcron/cmd/internal/svc"
	"rainiot/pkg/cache"

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

func (l *IotwsBalancedLogic) IotwsBalanced() {
	keys, err := l.svcCtx.Redis.Keys(l.ctx, cache.GetWsBalancedPublishCache("")).Result()
	if err != nil {
		return
	}
	wsBalancedMap := make(map[string]int, len(keys))
	totalws := 0
	for _, key := range keys {
		amount, err := l.svcCtx.Redis.GetDel(l.ctx, key).Result()
		if err != nil {
			l.Logger.Error(fmt.Sprintf("getdel error: %v", err))
			continue
		}
		if amount == "" {
			continue
		}
		wsBalancedMap[key], err = strconv.Atoi(amount)
		if err != nil {
			l.Logger.Error(fmt.Sprintf("atoi error: %v", err))
			continue
		}
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

	for disconnectWsserverName, val := range disconnectWsOutreceiveSeverMap {
		pub := cache.WsBalancedPublish{
			Meanws:                int64(meanws),
			ReceiveWsserver:       val.ReceiveWsserver,
			ReceiveWsserverAmount: val.ReceiveWsserverAmount,
		}
		l.svcCtx.Redis.Publish(l.ctx, cache.GetWsBalancedPublishCache(disconnectWsserverName), pub)
	}

}
