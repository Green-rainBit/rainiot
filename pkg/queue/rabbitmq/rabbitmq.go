package rabbitmq

import (
	"context"
	"fmt"
	"rainiot/pkg/openconfig"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/wagslane/go-rabbitmq"
)

type rabbitmqcilent struct {
	conn      *rabbitmq.Conn
	publisher *rabbitmq.Publisher
	exchange  string
	kind      string
	mu        sync.Mutex
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func NewRabbitMQConnect(rconfig openconfig.RabbitMQConfig, logger watermill.LoggerAdapter) (*rabbitmqcilent, error) {
	resolver := rabbitmq.NewStaticResolver(
		rconfig.Addresses,
		true, // shuffle 参数设为 true 可实现简单的客户端负载均衡
	)

	conn, err := rabbitmq.NewClusterConn(
		resolver,
		rabbitmq.WithConnectionOptionsReconnectInterval(5*time.Second),
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		return nil, err
	}
	return &rabbitmqcilent{
		conn: conn,
	}, nil

}

// Publish 发布 Watermill 消息
func (q *rabbitmqcilent) Publish(topic string, messages ...*message.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("queue is closed")
	}

	for _, msg := range messages {
		// 将 Watermill 的 Metadata 转换为 AMQP Headers
		headers := make(map[string]interface{})
		for k, v := range msg.Metadata {
			headers[k] = v
		}
		// 也可以添加自定义字段，如 content-type
		headers["content-type"] = "application/octet-stream"

		err := q.publisher.Publish(
			msg.Payload,
			[]string{topic},
			rabbitmq.WithPublishOptionsExchange(q.exchange),
			rabbitmq.WithPublishOptionsHeaders(headers),
			rabbitmq.WithPublishOptionsPersistentDelivery,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// Subscribe 订阅并返回 Watermill 消息通道
// func (q *rabbitmqcilent) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
// 	// 内部通道，用于将 AMQP Delivery 转换为 Watermill Message
// 	ch := make(chan *message.Message, 100)

// 	// 创建消费者（工作队列模式：所有消费者共享同一队列）
// 	consumer, err := rabbitmq.NewConsumer(
// 		q.conn,
// 		topic, // 固定队列名
// 		rabbitmq.WithConsumerOptionsExchangeName(q.exchange),
// 		rabbitmq.WithConsumerOptionsExchangeKind(q.kind),
// 		rabbitmq.WithConsumerOptionsExchangeDeclare,
// 		rabbitmq.WithConsumerOptionsQueueDurable,
// 		rabbitmq.WithConsumerOptionsConcurrency(5),
// 		rabbitmq.WithConsumerOptionsQOSPrefetch(10),
// 	)
// 	if err != nil {
// 		close(ch)
// 		return nil, err
// 	}

// 	// 启动消费循环
// 	q.wg.Add(1)
// 	go func() {
// 		defer q.wg.Done()
// 		defer close(ch)
// 		defer consumer.Close()

// 		// 这里使用 StartConsuming 的循环，当 ctx 取消时停止
// 		err := consumer.Run(
// 			func(d rabbitmq.Delivery) rabbitmq.Action {
// 				// 构造 Watermill Message
// 				msg := message.NewMessage(watermill.NewUUID(), d.Body)
// 				// 复制 Headers 到 Metadata
// 				for k, v := range d.Headers {
// 					if str, ok := v.(string); ok {
// 						msg.Metadata.Set(k, str)
// 					}
// 				}

// 				select {
// 				case ch <- msg:
// 					select {
// 					case <-msg.Acked():
// 						return rabbitmq.Ack
// 					case <-msg.Nacked():
// 						return rabbitmq.NackRequeue
// 					}
// 				case <-ctx.Done():
// 					return rabbitmq.NackRequeue
// 				}
// 			},
// 		)
// 		if err != nil && err != context.Canceled {
// 			// 记录错误，可考虑重试
// 		}
// 	}()

// 	return ch, nil
// }

// Subscribe 订阅主题，返回消息通道（工作队列模式：所有订阅者共享同一队列）
func (c *rabbitmqcilent) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ch := make(chan *message.Message, 100) // 带缓冲通道

	// 合并外部 ctx 和内部 ctx，任一取消则退出
	ctx, cancel := context.WithCancel(c.ctx)
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-c.ctx.Done():
			cancel()
		}
	}()
	// 等待外部 ctx 取消
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-c.ctx.Done():
			cancel()
		}
	}()

	c.wg.Add(1)
	go c.consumeLoop(ctx, topic, ch, cancel)

	return ch, nil
}

// consumeLoop 内部消费循环，自动重连
func (c *rabbitmqcilent) consumeLoop(ctx context.Context, topic string, ch chan *message.Message, cancel context.CancelFunc) {
	defer c.wg.Done()
	defer close(ch)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 创建消费者（每次重连重新创建）
		consumer, err := rabbitmq.NewConsumer(
			c.conn,
			topic, // 固定队列名，实现工作队列
			rabbitmq.WithConsumerOptionsExchangeName(c.exchange),
			rabbitmq.WithConsumerOptionsExchangeDeclare,
			rabbitmq.WithConsumerOptionsQueueDurable,
			rabbitmq.WithConsumerOptionsQOSPrefetch(10),
		)
		if err != nil {
			// 创建失败，等待后重试
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}

		// 开始消费
		err = consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			msg := message.NewMessage(watermill.NewUUID(), d.Body)
			// 复制 Headers 到 Metadata
			for k, v := range d.Headers {
				if str, ok := v.(string); ok {
					msg.Metadata.Set(k, str)
				}
			}
			select {
			case <-msg.Acked():
				return rabbitmq.Ack
			case <-msg.Nacked():
				return rabbitmq.NackRequeue
			}
		}) // 并发数，可根据需要调整

		// 关闭消费者（无论成功还是失败）
		consumer.Close()

		if err != nil {
			// 消费出错，短暂等待后重试
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		// 正常退出（ctx 被取消）
		return
	}
}

// Close 关闭所有资源
func (q *rabbitmqcilent) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	q.cancel()  // 停止所有订阅协程
	q.wg.Wait() // 等待所有协程结束
	if q.publisher != nil {
		q.publisher.Close()
	}
	if q.conn != nil {
		return q.conn.Close()
	}
	return nil
}

// Close 关闭客户端，释放资源
