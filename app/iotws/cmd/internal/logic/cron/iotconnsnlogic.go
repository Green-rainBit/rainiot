// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package logic

import (
	"context"
	"errors"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"

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
	instanceId := l.svcCtx.GetInstanceId()
	connectionIds := []string{}
	l.svcCtx.Connection.Range(func(key, value interface{}) bool {
		switch v := key.(type) {
		case string:
			connectionIds = append(connectionIds, v)
		}
		return true
	})
	_, failConnectionIds, err := l.setIfExistsBatchResult(l.ctx, connectionIds, instanceId)
	if err != nil {
		l.Logger.Error("Iotsync redis command error: %v", err)
		return
	}
	for _, failConnectionId := range failConnectionIds {
		conn, ok := l.svcCtx.Connection.GetConnByConnId(failConnectionId)
		if !ok {
			continue
		}
		switch co := conn.(type) {
		case *gws.Conn:
			co.NetConn().Close()
			continue
		default:
			// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
		}
		l.svcCtx.Connection.Delete(failConnectionId, conn)
	}
}

// SetIfExistsBatchResult 批量设置，并返回成功和失败的 key 信息
// 成功：设置成功（key存在）
// 失败：key 不存在，未设置值
func (l *iotconnsnlogic) setIfExistsBatchResult(ctx context.Context, connectionIds []string, instanceId string) (
	successKeys []string,
	failConnectionIds []string,
	err error,
) {
	pipe := l.svcCtx.Redis.Pipeline()
	cmds := make([]*redis.Cmd, len(connectionIds))
	// value 是 cache.Login 的 JSON（{"serviceName":"<instanceId>",...}），
	// 需要按 serviceName 比较；同时兼容历史遗留的纯 instanceId 字符串。
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

	_, err = pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		// Pipeline 本身出现的致命错误（网络等）
		return nil, nil, err
	}

	// 逐个检查命令返回值：1=续期成功，-1=key 不存在，0=已被其他实例接管
	for i, cmd := range cmds {
		result, cmdErr := cmd.Int64()
		if cmdErr != nil {
			l.Logger.Error("setIfExistsBatchResult redis command error for key %s: %v", cache.GetCacheConn(connectionIds[i]), cmdErr)
			continue
		}
		if result == 1 {
			successKeys = append(successKeys, connectionIds[i])
			continue
		}

		failConnectionIds = append(failConnectionIds, connectionIds[i])
	}
	return successKeys, failConnectionIds, nil
}
