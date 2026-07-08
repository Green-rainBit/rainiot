package natshandler

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/zeromicro/go-zero/core/logx"
)

func StartRouter(svcCtx *svc.ServiceContext) {
	logger := logx.WithContext(context.Background())
	ctx := context.Background()
	consumer, err := svcCtx.Queue.Subscribe(ctx, svcCtx.Config.MQ.NATS.Subject)
	if err != nil {
		logger.Errorf("[NATS] 创建消费者失败: %v", err)
		return
	}
	logger.Infof("[NATS] 消费者启动成功: subject=%s", svcCtx.Config.MQ.NATS.Subject)

	// 单消费者直接路由：HandleAllNatsMessage 内部按 cmd 分发到对应 logic，
	// 消除了原来的 "接收→提取 cmd→重新发布到 subject_login→再次消费" 的二次 NATS 往返。
	go func() {
		for msg := range consumer {
			go func(ms *message.Message) {
				err := HandleAllNatsMessage(svcCtx, ms)
				if err != nil {
					logger.Errorf("[NATS] 处理消息失败: %v", err)
					ms.Nack()
					return
				}
				ms.Ack()
			}(msg)
		}
	}()
}
