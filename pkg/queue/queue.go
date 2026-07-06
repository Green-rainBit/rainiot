package queue

import (
	"context"
	"fmt"
	"rainiot/pkg/openconfig"

	"github.com/hadi77ir/go-mq"
	nats "github.com/hadi77ir/go-mq/nats"
	"github.com/hadi77ir/go-mq/rabbitmq"
)

// NewBrokerFromConfig 根据配置创建对应的 Broker 实例
func NewBrokerFromConfig(ctx context.Context, cfg *openconfig.MQConfig) (mq.Broker, error) {
	switch cfg.Type {
	case "rabbitmq":
		rmqCfg := rabbitmq.Config{
			Connection: mq.Config{
				Addresses: cfg.RabbitMQ.Addresses,
				Username:  cfg.RabbitMQ.Username,
				Password:  cfg.RabbitMQ.Password,
			},
			Exchange:        cfg.RabbitMQ.Exchange,
			ExchangeType:    cfg.RabbitMQ.ExchangeType,
			DeclareExchange: cfg.RabbitMQ.DeclareExchange,
		}
		return rabbitmq.NewBroker(ctx, rmqCfg)

	case "nats":
		natsCfg := nats.Config{
			Connection: mq.Config{
				Addresses: cfg.NATS.Addresses,
				Username:  cfg.NATS.Username,
				Password:  cfg.NATS.Password,
			},
			PublishMode: nats.PublishModeJetStream,
			// 可添加 JetStream 等高级配置
		}
		return nats.NewBroker(ctx, natsCfg)

	default:
		return nil, fmt.Errorf("unsupported MQ type: %s", cfg.Type)
	}
}
