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
	Push(ctx context.Context, connId string, message []byte) ([]byte, bool, error)
}

type wsCli struct {
	httpCli WsCliInterface
	redis   *redis.ClusterClient
}

func NewWsCli(serviceName string, confCli configcli.ConfigCli) *wsCli {
	wsCli := &wsCli{
		httpCli: httpc.NewDeviceCli("", confCli.GetHealthyInstances),
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, serviceName, event string, req Request, Resp any) error {
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
		d.httpCli.Push(ctx, event, jsonBytes)
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
