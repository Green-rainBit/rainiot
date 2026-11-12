package wscli

import (
	"context"
	"encoding/json"
	"errors"

	"rainiot/pkg/client/wscli/httpc"

	"github.com/redis/go-redis/v9"
)

const (
	eventQueue = "queue"
	eventHttp  = "http"
)

type WsCliInterface interface {
	Push(ctx context.Context, event string, req Request, Resp any) error
}

type wsCli struct {
	httpCli httpc.WsHttpCliInterface
	redis   *redis.ClusterClient
}

func NewWsCli(serviceName string, rdb *redis.ClusterClient) *wsCli {
	wsCli := &wsCli{
		httpCli: httpc.NewDeviceCli(serviceName),
		redis:   rdb,
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, event string, req Request, resp any) error {
	if d.redis == nil {
		return errors.New("wscli: redis client is nil")
	}

	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var sub *redis.PubSub
	if resp != nil {
		sub = d.redis.Subscribe(ctx, event+"Ack"+req.MessgeId)
		defer sub.Close()
	}

	switch event {
	case eventQueue:
		// todo

	case eventHttp:
		_, err = d.httpCli.Push(ctx, event, jsonBytes)
		if err != nil {
			return err
		}
	}
	if resp != nil {
		redisMessage, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		err = json.Unmarshal([]byte(redisMessage.Payload), resp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *wsCli) GetWsAddresse(string) []string {
	return d.redis.Get
}
