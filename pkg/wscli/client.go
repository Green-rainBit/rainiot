package wscli

import (
	"context"
	"encoding/json"

	"rainiot/pkg/configcli"
	"rainiot/pkg/wscli/httpc"
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
}

func NewWsCli(serviceName string, confCli configcli.ConfigCli) *wsCli {
	wsCli := &wsCli{
		httpCli: httpc.NewDeviceCli(serviceName, confCli.GetHealthyInstances),
	}
	return wsCli
}

func (d *wsCli) Push(ctx context.Context, serviceName, event string, req Request) ([]byte, bool, error) {
	jsonBytes, err := json.Marshal(req)
	if err != nil {
		return nil, false, err
	}
	switch event {
	case eventQueue:
		// todo
		return nil, false, err
	case eventHttp:
		return d.httpCli.Push(ctx, event, jsonBytes)
	}
	return nil, false, err
}
