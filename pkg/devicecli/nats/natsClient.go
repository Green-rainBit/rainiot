package nats

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// NatsConf 配置 NATS 连接和发布行为。
type NatsConf struct {
	// Urls 是 NATS 服务器地址列表，支持集群高可用。
	// 例如: ["nats://host1:4222", "nats://host2:4222"]
	// 第一个成功连接的地址将被使用，其余作为故障转移。
	// 如果为空，使用 nats.DefaultURL。
	Urls []string `json:",optional"`

	// Subject 是发布消息时要使用的默认主题。
	// 如果为空，使用 serviceName 作为回退。
	Subject string `json:",optional"`

	// MaxReconnects 是在放弃之前的最大重连尝试次数。
	// 默认值: nats.DefaultMaxReconnect (60)。
	MaxReconnects int `json:",optional"`

	// ReconnectWait 是两次重连尝试之间的等待时间（秒）。
	// 默认值: nats.DefaultReconnectWait (2s)。
	ReconnectWait int `json:",optional"`

	// ConnectionTimeout 是连接尝试的超时时间（秒）。
	// 默认值: 5 秒。
	ConnectionTimeout int `json:",optional"`

	// JetStream 是否启用 JetStream 持久化发布。
	// 启用后消息将由 NATS JetStream 持久化存储，
	// 即使 NATS 服务重启也不会丢失消息。
	// 需要 NATS 服务端启用 JetStream（-js 参数）。
	JetStream bool `json:",optional"`
}

const (
	defaultRetryCount = 3
	defaultRetryWait  = 1 * time.Second
	defaultTimeout    = 5 * time.Second
)

// ErrNatsNotConnected NATS 未连接时返回的错误。
var ErrNatsNotConnected = nats.ErrConnectionClosed

type deviceNatsCli struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	serviceName string
	subject     string
	jetStream   bool
	retryCount  int
	retryWait   time.Duration
}

// NewDeviceCli 创建 NATS 设备客户端，实现 devicecli.DeviceCli 接口。
func NewDeviceCli(serviceName string, conf NatsConf) (*deviceNatsCli, error) {
	urls := conf.Urls
	if len(urls) == 0 {
		urls = []string{nats.DefaultURL}
	}

	maxReconnects := conf.MaxReconnects
	if maxReconnects <= 0 {
		maxReconnects = nats.DefaultMaxReconnect
	}

	reconnectWait := time.Duration(conf.ReconnectWait) * time.Second
	if reconnectWait <= 0 {
		reconnectWait = nats.DefaultReconnectWait
	}

	timeout := time.Duration(conf.ConnectionTimeout) * time.Second
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	opts := []nats.Option{
		nats.MaxReconnects(maxReconnects),
		nats.ReconnectWait(reconnectWait),
		nats.Timeout(timeout),
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[NATS] Disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Printf("[NATS] Reconnected")
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Printf("[NATS] Connection closed")
		}),
	}

	// nats.Connect 接受逗号分隔的多个 URL，自动支持集群发现和故障转移
	conn, err := nats.Connect(strings.Join(urls, ","), opts...)
	if err != nil {
		return nil, err
	}

	subject := conf.Subject
	if subject == "" {
		subject = serviceName
	}

	d := &deviceNatsCli{
		conn:        conn,
		serviceName: serviceName,
		subject:     subject,
		jetStream:   conf.JetStream,
		retryCount:  defaultRetryCount,
		retryWait:   defaultRetryWait,
	}

	// 如果启用 JetStream，初始化 JetStream 上下文
	if conf.JetStream {
		js, err := conn.JetStream()
		if err != nil {
			conn.Close()
			return nil, err
		}
		d.js = js
		log.Printf("[NATS] JetStream enabled for subject=%q", subject)
	}

	log.Printf("[NATS] Connected to %s", conn.ConnectedUrl())
	return d, nil
}

// Push 将消息发布到 NATS 主题（即发即弃模式）。
// connId 通过 NATS 头部传递，message 作为消息体。
// 如果启用了 JetStream，使用 js.PublishMsg 持久化发布。
func (d *deviceNatsCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	if d.conn == nil || !d.conn.IsConnected() {
		return nil, ErrNatsNotConnected
	}

	msg := &nats.Msg{
		Subject: d.subject,
		Data:    message,
		Header:  nats.Header{},
	}
	msg.Header.Set("ConnId", connId)
	msg.Header.Set("ServiceName", d.serviceName)

	var err error
	for i := 0; i <= d.retryCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if d.jetStream {
			_, err = d.js.PublishMsg(msg)
		} else {
			err = d.conn.PublishMsg(msg)
		}
		if err == nil {
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.retryWait):
		}
	}
	return nil, err
}

// Close 优雅关闭 NATS 连接。
func (d *deviceNatsCli) Close() {
	if d.conn != nil {
		_ = d.conn.Drain()
		d.conn.Close()
	}
}
