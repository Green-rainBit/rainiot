package natshandler

import (
	"bytes"

	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/ThreeDotsLabs/watermill/message"
)

// HandleAllNatsMessage 入口消费者回调：从消息里提取 cmd，按 cmd 重新发布到专属 subject。
// 二次分发是为了利用 NATS 队列组实现 cmd 间隔离——慢 cmd 不会阻塞快 cmd 的消息投递。
func HandleAllNatsMessage(svcCtx *svc.ServiceContext, msg *message.Message) error {
	cmd := extractCmd(msg.Payload)
	if cmd == "" {
		msg.Ack()
		return nil
	}
	subject := svcCtx.Config.MQ.NATS.Subject

	// 检查 cmd 是否已注册（未注册的 cmd 丢弃并 Ack）
	if _, ok := cmdWorkers[cmd]; !ok {
		msg.Ack()
		return nil
	}

	newSubject := subject + "_" + cmd
	if err := svcCtx.Queue.Publish(newSubject, msg); err != nil {
		return err
	}
	msg.Ack()
	return nil
}

// extractCmd 从 JSON 负载里取出 "cmd" 字段值；缺失或非法时返回 ""。
func extractCmd(payload []byte) string {
	key := []byte(`"cmd":"`)
	i := bytes.Index(payload, key)
	if i == -1 {
		return ""
	}
	start := i + len(key)
	end := bytes.IndexByte(payload[start:], '"')
	if end == -1 {
		return ""
	}
	return string(payload[start : start+end])
}
