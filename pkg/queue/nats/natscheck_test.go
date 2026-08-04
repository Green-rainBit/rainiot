//go:build ignore

package main

import (
	"context"
	"fmt"
	"time"
	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect("nats://admin:admin@127.0.0.1:14222",
		nats.MaxReconnects(3),
		nats.ReconnectWait(1*time.Second),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("CONNECT ERROR: %v\n", err)
		return
	}
	defer nc.Close()
	fmt.Printf("Connected to NATS: %s\n", nc.ConnectedUrl())

	js, err := nc.JetStream()
	if err != nil {
		fmt.Printf("JETSTREAM ERROR: %v\n", err)
		return
	}
	fmt.Println("JetStream context obtained")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.PublishMsg(&nats.Msg{
		Subject: "iot_device",
		Data:    []byte(`{"test":"hello"}`),
	}, nats.Context(ctx))
	if err != nil {
		fmt.Printf("PUBLISH ERROR: %v\n", err)
	} else {
		fmt.Println("PUBLISH SUCCESS")
	}

	stream, err := js.StreamInfo("iot_device")
	if err != nil {
		fmt.Printf("STREAM INFO ERROR: %v\n", err)
	} else {
		fmt.Printf("Stream: %s, Messages: %d\n", stream.Config.Name, stream.State.Msgs)
	}
}
