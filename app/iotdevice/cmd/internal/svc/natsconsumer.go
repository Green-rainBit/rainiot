package svc

import (
	"log"
	"strings"
	"time"

	"rainiot/pkg/devicecli/nats"

	natsio "github.com/nats-io/nats.go"
)

// NatsConsumer NATS 消息消费者，通过 QueueSubscribe 支持多实例负载均衡。
type NatsConsumer struct {
	conn *natsio.Conn
}

// NewNatsConsumer 创建 NATS 消费者，连接并订阅主题。
//   - 支持 NATS 集群：通过 conf.Urls 指定多个节点地址
//   - 支持多实例负载均衡：使用 QueueSubscribe 自动分发消息
//   - 支持自动重连：断线后自动恢复连接和订阅
//
// handler 为消息处理回调，由调用方注入业务逻辑。
func NewNatsConsumer(conf nats.NatsConf, handler func(msg *natsio.Msg)) (*NatsConsumer, error) {
	urls := conf.Urls
	if len(urls) == 0 {
		urls = []string{natsio.DefaultURL}
	}

	maxReconnects := conf.MaxReconnects
	if maxReconnects <= 0 {
		maxReconnects = natsio.DefaultMaxReconnect
	}

	reconnectWait := conf.ReconnectWait
	if reconnectWait <= 0 {
		reconnectWait = int(natsio.DefaultReconnectWait.Seconds())
	}

	timeout := conf.ConnectionTimeout
	if timeout <= 0 {
		timeout = 5
	}

	opts := []natsio.Option{
		natsio.MaxReconnects(maxReconnects),
		natsio.ReconnectWait(time.Duration(reconnectWait) * time.Second),
		natsio.Timeout(time.Duration(timeout) * time.Second),
		natsio.RetryOnFailedConnect(true),
		natsio.DisconnectErrHandler(func(_ *natsio.Conn, err error) {
			log.Printf("[NATS Consumer] Disconnected: %v", err)
		}),
		natsio.ReconnectHandler(func(_ *natsio.Conn) {
			log.Printf("[NATS Consumer] Reconnected")
		}),
		natsio.ClosedHandler(func(_ *natsio.Conn) {
			log.Printf("[NATS Consumer] Connection closed")
		}),
	}

	// nats.Connect 接受逗号分隔的多个 URL，自动支持集群发现和故障转移
	conn, err := natsio.Connect(strings.Join(urls, ","), opts...)
	if err != nil {
		return nil, err
	}

	subject := conf.Subject
	if subject == "" {
		subject = "iotdevice"
	}

	// QueueSubscribe: 同一 queue group 的多实例自动负载均衡
	_, err = conn.QueueSubscribe(subject, "iotdevice-group", handler)
	if err != nil {
		conn.Close()
		return nil, err
	}

	log.Printf("[NATS Consumer] Subscribed to subject=%q queue=%q, connected=%s",
		subject, "iotdevice-group", conn.ConnectedUrl())
	return &NatsConsumer{conn: conn}, nil
}

// Close 优雅关闭 NATS 消费者连接。
func (c *NatsConsumer) Close() {
	if c.conn != nil {
		_ = c.conn.Drain()
		c.conn.Close()
		log.Printf("[NATS Consumer] Connection closed")
	}
}
