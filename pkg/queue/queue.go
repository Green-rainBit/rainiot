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
	if cfg.Type == "" {
		return nil, nil
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("mq topic is required when mq.type=%q", cfg.Type)
	}
	logger := watermill.NewStdLogger(false, false)

	switch cfg.Type {
	case "rabbitmq":
		queue, err := rabbitmq.NewRabbitMQConnect(cfg.RabbitMQ, logger)
		if err != nil {
			return nil, err
		}
		return queue, nil
	case "nats":
		queue, err := nats.NewNatsConnect(cfg.NATS, cfg.Topic, logger)
		if err != nil {
			return nil, err
		}
		return queue, nil

	default:
		return nil, fmt.Errorf("unsupported MQ type: %s", cfg.Type)
	}
}
