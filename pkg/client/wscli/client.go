package wscli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"rainiot/pkg/cache"
	"rainiot/pkg/client/wscli/httpc"

	goredis "github.com/redis/go-redis/v9"
)

const (
	eventQueue = "queue"
	eventHttp  = "http"
	eventCache = "cache"
)

type WsCliInterface interface {
	Push(ctx context.Context, instanceId, event, connId string, req Request, Resp any) error
}

type wsCli struct {
	httpCli httpc.WsHttpCliInterface
	cache   cache.CacheCliDevice
}

func NewWsCli(rdb cache.CacheCliDevice) *wsCli {
	wsCli := &wsCli{
		httpCli: httpc.NewDeviceCli(),
		cache:   rdb,
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, instanceId, event, connId string, req Request, resp any) error {
	if d.cache == nil {
		return errors.New("wscli: redis client is nil")
	}

	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var sub <-chan *goredis.Message
	if resp != nil {
		sub = d.cache.SubscribeByEventMessgId(ctx, event, req.MessgeId)
	}
	model := ""
	switch model {
	case eventQueue:
	// todo

	case eventCache:
		bytest := strings.Builder{}
		_, err = bytest.WriteString(connId + "|")
		if err != nil {
			return err
		}
		_, err = bytest.WriteString(event + "|")
		if err != nil {
			return err
		}
		_, err = bytest.Write(jsonBytes)
		if err != nil {
			return err
		}
		// okSub := d.cache.SubscribeByMessgId(ctx, req.MessgeId)
		err = d.cache.PublishMsgByInstanceId(ctx, instanceId, bytest.String())
		if err != nil {
			return err
		}
		// <-okSub

	case eventHttp:
		addr, ok, err := d.cache.GetAddrByInstanceId(ctx, instanceId)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("wscli: instanceId not found")
		}
		_, err = d.httpCli.Push(ctx, addr, event, connId, jsonBytes)
		if err != nil {
			return err
		}
	}
	if resp != nil {
		redisMessage, ok := <-sub
		if !ok {
			return errors.New("wscli: sub channel closed")
		}
		err = json.Unmarshal([]byte(redisMessage.Payload), resp)
		if err != nil {
			return err
		}
	}

	return nil
}
