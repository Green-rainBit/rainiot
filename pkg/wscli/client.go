package wscli

import (
	"context"
	"encoding/json"

	"rainiot/pkg/configcli"
	"rainiot/pkg/wscli/httpc"

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

func NewWsCli(serviceName string, confCli configcli.ConfigCli) *wsCli {
	wsCli := &wsCli{
		httpCli: httpc.NewDeviceCli(serviceName, confCli.GetHealthyInstances),
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, event string, req Request, Resp any) error {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}

	sub := d.redis.Subscribe(ctx, event+"Ack"+req.MessgeId)
	defer sub.Close()

	switch event {
	case eventQueue:
		// todo

	case eventHttp:
		_, err = d.httpCli.Push(ctx, event, jsonBytes)
		if err != nil {
			return err
		}
	}
	redisMessage, err := sub.ReceiveMessage(ctx)
	if err != nil {
		return err
	}
	err = json.Unmarshal([]byte(redisMessage.Payload), Resp)
	if err != nil {
		return err
	}
	return nil
}
