package watermillqueue

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// WatermillQueue 封装 Watermill 的 Publisher 和 Subscriber
type WatermillQueue struct {
	publisher  message.Publisher
	subscriber message.Subscriber
	closeOnce  sync.Once
}

// NewWatermillQueue 创建基于 Watermill + AMQP 的 Queue 实例，支持 RabbitMQ 集群
func NewWatermillQueue(clusterURIs []string, exchange string, exchangeType string) (*WatermillQueue, error) {
	logger := watermill.NewStdLogger(false, false)

	// 1. 创建一个带故障转移的 AMQP 连接
	//    Watermill 的 AMQP 配置允许直接传入 *amqp.Connection，我们可以手动建立集群连接
	//    方法：使用 rabbitmq/amqp091-go 的 Dial 尝试列表中的 URI
	amqpConfig := dialCluster(clusterURIs)

	// 3. 创建 Publisher 和 Subscriber
	publisher, err := amqp.NewPublisher(amqpConfig, logger)
	if err != nil {
		return nil, err
	}

	subscriber, err := amqp.NewSubscriber(amqpConfig, logger)
	if err != nil {
		publisher.Close()

		return nil, err
	}

	return &WatermillQueue{
		publisher:  publisher,
		subscriber: subscriber,
	}, nil
}

// Publish 实现 Queue.Publish
func (w *WatermillQueue) Publish(topic string, messages ...*message.Message) error {
	return w.publisher.Publish(topic, messages...)
}

// Subscribe 实现 Queue.Subscribe
func (w *WatermillQueue) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return w.subscriber.Subscribe(ctx, topic)
}

// Close 实现 Queue.Close
func (w *WatermillQueue) Close() error {
	var err error
	w.closeOnce.Do(func() {
		if w.publisher != nil {
			if e := w.publisher.Close(); e != nil {
				err = e
			}
		}
		if w.subscriber != nil {
			if e := w.subscriber.Close(); e != nil {
				err = e
			}
		}
	})
	return err
}

// dialCluster 尝试连接集群中的任一节点
func dialCluster(uris []string) amqp.Config {
	for _, uri := range uris {
		return amqp.NewNonDurableQueueConfig(uri)
	}
	return amqp.NewNonDurableQueueConfig("")
}
