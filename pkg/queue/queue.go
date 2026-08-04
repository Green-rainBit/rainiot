package queue

import (
	"context"
	"fmt"

	"rainiot/pkg/openconfig"
	nats "rainiot/pkg/queue/nats"
	"rainiot/pkg/queue/rabbitmq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// NewBrokerFromConfig 根据配置创建对应的 Broker 实例

type Queue interface {
	// 创建 Broker 实例
	Publish(topic string, messages ...*message.Message) error
	Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error)
	Close() error
}

func NewBrokerFromConfig(ctx context.Context, cfg *openconfig.MQConfig) (Queue, error) {
	logger := watermill.NewStdLogger(false, false)
	switch cfg.Type {
	case "rabbitmq":
		queue, err := rabbitmq.NewRabbitMQConnect(cfg.RabbitMQ, logger)
		return queue, err
	case "nats":
		return nats.NewNatsConnect(cfg.NATS, logger)

	default:
		return nil, fmt.Errorf("unsupported MQ type: %s", cfg.Type)
	}
}
