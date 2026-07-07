package natshandler

import (
	"context"
	"log"

	"rainiot/app/iotdevice/cmd/internal/svc"
)

func StartRouter(svcCtx *svc.ServiceContext) {
	ctx := context.Background()
	consumer, err := svcCtx.Queue.Subscribe(ctx, "iot_device")
	if err != nil {
		log.Printf("[NATS] 创建消费者失败: %v", err)
		return
	}
	for msg := range consumer {
		msg.Ack()
	}
	//(context.Background(), topic string, queue string, consumerName string, opts ...mq.Option)
}
