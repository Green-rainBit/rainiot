package natshandler

import (
	"context"
	"log"
	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/nats-io/nats.go/jetstream"
)

// 分流器启动函数
func StartRouter(serverCtx *svc.ServiceContext) {
	// 2. 启动消费循环
	ctx := context.Background()
	stream, err := serverCtx.NatsJetStream.Stream(ctx, "raw.ingress")
	if err != nil {
		log.Printf("创建流失败: %v", err)
		return
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       serverCtx.Config.Nats.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 100, // 控制并发拉取数量
	})
	consumer.Consume(func(msg jetstream.Msg) {
		HandleAllNatsMessage(ctx, serverCtx.Config.Nats, serverCtx.NatsJetStream, msg)
	})
	NewConsumer(ctx, serverCtx, stream)
}

func NewConsumer(ctx context.Context, serverCtx *svc.ServiceContext, stream jetstream.Stream) error {
	cmds := []string{
		"device.connect",
		"device.disconnect",
		"device.login",
		"device.logout",
		"device.error",
		"device.info",
		"device.push",
		"device.reload",
	}
	consumers := []jetstream.Consumer{}
	for _, cmd := range cmds {
		consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:       serverCtx.Config.Nats.Subject + cmd,
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxAckPending: 100, // 控制并发拉取数量
		})
		if err != nil {
			return err
		}
		consumers = append(consumers, consumer)
	}
	for _, consumer := range consumers {
		consumer.Consume(func(msg jetstream.Msg) {
			HandleNatsMessage(msg, serverCtx)
		})
	}
	return nil
}
