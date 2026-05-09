// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"rainiot/app/iotcron/cmd/internal/svc"
	"strconv"

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
	raw, err := l.svcCtx.Redis.HGetAll(l.ctx, "key").Result()
	if err != nil {
		return
	}
	wsBalancedMap := make(map[string]int, len(raw))
	totalws := 0
	for k, v := range raw {
		n, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		totalws += n
		wsBalancedMap[k] = n
	}
	meanws := totalws / len(wsBalancedMap)
	disconnectWsserver := map[string]int{}
	receiveWsserver := map[string]int{}
	offset := 0
	if meanws/5 > 20 {
		offset = meanws / 5
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
	disconnectWsOutreceiveSeverMap := map[string][]string{}
	for serverName, totalws := range disconnectWsserver {
		receiveWsserverNames := []string{}
		for receiveWsserverName, receiveWs := range receiveWsserver {
			if receiveWs == 0 {
				continue
			}
			if receiveWs < totalws {
				totalws -= receiveWs
				delete(receiveWsserver, receiveWsserverName)
			} else {
				receiveWsserver[receiveWsserverName] = receiveWs - totalws
			}
			receiveWsserverNames = append(receiveWsserverNames, receiveWsserverName)
		}
		disconnectWsOutreceiveSeverMap[serverName] = receiveWsserverNames
	}
	for disconnectWsserverName, receiveWsserverNames := range disconnectWsOutreceiveSeverMap {
		l.svcCtx.Redis.Publish(l.ctx, disconnectWsserverName, receiveWsserverNames)
	}

}
