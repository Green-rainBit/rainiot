package natshandler

import (
	"bytes"
	"log"
	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/ThreeDotsLabs/watermill/message"
)

// // HandleAllNatsMessage 是路由消费者回调：从原始入口消息里提取 cmd，
// // 把消息以 "<ingress>.<cmd>" 主题重新发布到同一个流，供各 cmd 下游消费者分别处理。
func HandleAllNatsMessage(svcCtx *svc.ServiceContext, msg *message.Message) error {
	cmd := extractCmd(msg.Payload)
	log.Printf("[NATS]: %s", string(msg.Payload))
	if cmd == "" {
		return nil
	}
	logicMap := map[string]struct{}{}
	logicMap["login"] = struct{}{}
	if _, ok := logicMap[cmd]; !ok {
		return nil
	}

	// 发布主题必须用 "." 分隔，与流过滤器 (ingress+".>") 及下游 FilterSubject 精确匹配。
	// 切勿对主题使用 sanitizeName：它会把 "." 换成 "_"，导致主题匹配不到任何流，报 "no response from stream"。
	newSubject := svcCtx.Config.MQ.NATS.Subject + "_" + cmd
	if err := svcCtx.Queue.Publish(newSubject, msg); err != nil {
		log.Printf("[NATS] 分发失败 subject=%s: %v", newSubject, err)

		return err
	}
	log.Printf("[NATS] 已路由至 %s", newSubject)
	return nil
}

// // extractCmd 从 JSON 负载里取出 "cmd" 字段值；缺失或非法时返回 ""。
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
