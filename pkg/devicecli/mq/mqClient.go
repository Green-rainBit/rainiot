package mq

import (
	"context"
	"log"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/queue"
	"time"

	"github.com/hadi77ir/go-mq"
	"github.com/nats-io/nats.go"
)

const (
	defaultRetryCount = 3
	defaultRetryWait  = 1 * time.Second
	defaultTimeout    = 5 * time.Second
)

// ErrNatsNotConnected NATS 未连接时返回的错误。
var ErrNatsNotConnected = nats.ErrConnectionClosed

type deviceNatsCli struct {
	conn        mq.Broker
	serviceName string
	subject     string
	retryCount  int
	retryWait   time.Duration
}

// NewDeviceCli 创建 NATS 设备客户端，实现 devicecli.DeviceCli 接口。
func NewDeviceCli(serviceName string, cfg *openconfig.MQConfig) (*deviceNatsCli, error) {
	conn, err := queue.NewBrokerFromConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	log.Printf("[NATS] Connected to %s", conn)
	d := &deviceNatsCli{
		conn:        conn,
		serviceName: serviceName,
		retryCount:  defaultRetryCount,
		retryWait:   defaultRetryWait,
	}
	return d, nil
}

// Push 将消息发布到 NATS 主题（即发即弃模式）。
// connId 通过 NATS 头部传递，message 作为消息体。
// 如果启用了 JetStream，使用 js.PublishMsg 持久化发布。
func (d *deviceNatsCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	if d.conn == nil {
		return nil, ErrNatsNotConnected
	}

	msg := mq.Message{
		Body: message,
		Headers: map[string]string{
			"ServiceName": d.serviceName,
			"ConnId":      connId,
		},
		// Timeout: defaultTimeout,
	}

	var err error
	for i := 0; i <= d.retryCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		err = d.conn.Publish(ctx, "iot_device", msg)
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

func (d *deviceNatsCli) Information() string {
	return d.serviceName
}

// Close 优雅关闭 NATS 连接。
func (d *deviceNatsCli) Close() {
	if d.conn == nil {
		return
	}
	d.conn.Close(context.Background())
}
