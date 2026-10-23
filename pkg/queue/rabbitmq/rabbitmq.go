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
	conn            *rabbitmq.Conn
	publisher       *rabbitmq.Publisher
	exchange        string
	mu              sync.Mutex
	closed          bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	declareExchange bool
	exchangeType    string
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
	pubOpts := []func(*rabbitmq.PublisherOptions){
		rabbitmq.WithPublisherOptionsExchangeName(rconfig.Exchange),
	}
	if rconfig.ExchangeType != "" {
		pubOpts = append(pubOpts, rabbitmq.WithPublisherOptionsExchangeKind(rconfig.ExchangeType))
	} else {
		pubOpts = append(pubOpts, rabbitmq.WithPublisherOptionsExchangeKind("direct"))
	}
	if rconfig.Exchange != "" && rconfig.DeclareExchange {
		pubOpts = append(pubOpts,
			rabbitmq.WithPublisherOptionsExchangeDeclare,
			rabbitmq.WithPublisherOptionsExchangeDurable,
		)
	}
	publisher, err := rabbitmq.NewPublisher(
		conn,
		pubOpts...,
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	return &rabbitmqcilent{
		conn:            conn,
		publisher:       publisher,
		exchange:        rconfig.Exchange,
		exchangeType:    rconfig.ExchangeType,
		declareExchange: rconfig.DeclareExchange,
		mu:              sync.Mutex{},
		closed:          false,
		ctx:             runCtx,
		wg:              sync.WaitGroup{},
		cancel:          cancel,
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

// Subscribe 订阅主题，返回消息通道（工作队列模式：所有订阅者共享同一队列）
func (q *rabbitmqcilent) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ch := make(chan *message.Message, 100) // 带缓冲通道

	// 合并外部 ctx 和内部 ctx，任一取消则退出
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-q.ctx.Done():
			cancel()
		}
	}()

	q.wg.Add(1)
	go q.consumeLoop(ctx, topic, ch, cancel)

	return ch, nil
}

// consumeLoop 内部消费循环，自动重连
func (q *rabbitmqcilent) consumeLoop(ctx context.Context, topic string, ch chan *message.Message, cancel context.CancelFunc) {
	defer q.wg.Done()
	defer close(ch)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		opts := []func(*rabbitmq.ConsumerOptions){
			rabbitmq.WithConsumerOptionsExchangeName(q.exchange),
			rabbitmq.WithConsumerOptionsRoutingKey(topic),
			rabbitmq.WithConsumerOptionsQueueDurable,
			rabbitmq.WithConsumerOptionsQOSPrefetch(10),
		}
		if q.exchangeType == "" {
			opts = append(opts, rabbitmq.WithConsumerOptionsExchangeKind("direct"))
		} else {
			opts = append(opts, rabbitmq.WithConsumerOptionsExchangeKind(q.exchangeType))
		}
		if q.exchange != "" && q.declareExchange {
			opts = append(opts, rabbitmq.WithConsumerOptionsExchangeDeclare)
		}
		// 创建消费者（每次重连重新创建）
		consumer, err := rabbitmq.NewConsumer(q.conn, topic, opts...)

		if err != nil {
			// 创建失败，等待后重试
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
		var stopOnce sync.Once
		// 方案 A：关闭时给超时
		stop := func() {
			stopOnce.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				consumer.CloseWithContext(ctx)
			})
		}

		go func() {
			<-ctx.Done()
			stop()
		}()

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
		stop()
		// 关闭消费者（无论成功还是失败）

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
