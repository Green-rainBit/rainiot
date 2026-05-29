// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"
	"rainiot/pkg/util"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type iotconnsnlogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewIotconnsnlogic(ctx context.Context, svcCtx *svc.ServiceContext) *iotconnsnlogic {
	return &iotconnsnlogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *iotconnsnlogic) Iotsync() {
	serviceName, _, _ := util.GetRegistryParameters(l.svcCtx.Config.RestConf)
	connectionIds := []string{}
	l.svcCtx.Connection.Range(func(key, value interface{}) bool {
		switch v := key.(type) {
		case string:
			connectionIds = append(connectionIds, v)
		}
		return true
	})
	_, failConnectionIds, err := l.setIfExistsBatchResult(l.ctx, connectionIds, serviceName)
	if err != nil {
		return
	}
	for _, failConnectionId := range failConnectionIds {
		conn, ok := l.svcCtx.Connection.GetconnByConnId(failConnectionId)
		if !ok {
			continue
		}
		switch co := conn.(type) {
		case *gws.Conn:
			co.NetConn().Close()
			return
		default:
			// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
		}
		l.svcCtx.Connection.Del(failConnectionId)
	}
}

// SetIfExistsBatchResult 批量设置，并返回成功和失败的 key 信息
// 成功：设置成功（key存在）
// 失败：key 不存在，未设置值
func (l *iotconnsnlogic) setIfExistsBatchResult(ctx context.Context, connectionIds []string, serviceName string) (
	successKeys []string,
	failConnectionIds []string,
	err error,
) {
	pipe := l.svcCtx.Redis.Pipeline()
	cmds := make([]*redis.StatusCmd, len(connectionIds))
	for i, connectionId := range connectionIds {
		cmds[i] = pipe.SetArgs(ctx, cache.GetCacheConn(connectionId), serviceName, redis.SetArgs{
			Mode: "XX",
			TTL:  cache.ConnTime,
		})
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// Pipeline 本身出现的致命错误（网络等）
		return nil, nil, err
	}

	// 逐个检查命令状态
	for i, cmd := range cmds {
		if cmd.Err() == redis.Nil {
			// key 不存在，未设置
			failConnectionIds = append(failConnectionIds, connectionIds[i])
		} else if cmd.Err() != nil {
			l.Logger.Error("setIfExistsBatchResult redis command error for key %s: %v", cache.GetCacheConn(connectionIds[i]), cmd.Err())
			failConnectionIds = append(failConnectionIds, connectionIds[i])
		} else {
			// 成功
			successKeys = append(successKeys, connectionIds[i])
		}
	}
	return successKeys, failConnectionIds, nil
}
