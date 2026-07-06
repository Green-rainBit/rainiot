package natshandler

import (
	"context"
	"log"
	
	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/hadi77ir/go-mq"
)


func StartRouter(svcCtx *svc.ServiceContext) {
	ctx := context.Background()
	consumer, err := svcCtx.Mq.Consume(ctx, "iot_device", "worker-queue", "worker-1",
		mq.WithAutoAck(false),
		mq.WithPrefetch(10),
		mq.WithDeadLetterTopic("events.dlq"),
	)
	if err != nil {
		log.Printf("[NATS] 创建消费者失败: %v", err)
		return
	}
	for {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			break
		}
		log.Printf("[NATS] 收到消息: %s", delivery.Message.Body)

		// Process message
		//processMessage(delivery.Message)

		// Acknowledge
		if err := delivery.Ack(ctx); err != nil {
			// Handle error
			log.Printf("[NATS] 确认消息失败: %v", err)
		}
	}
	//(context.Background(), topic string, queue string, consumerName string, opts ...mq.Option)
}
