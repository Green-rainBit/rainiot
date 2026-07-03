package natshandler

import (
	"bytes"
	"context"
	"log"
	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"
	"rainiot/pkg/devicecli/nats"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

// HandleAllNatsMessage 是路由消费者回调：从原始入口消息里提取 cmd，
// 把消息以 "<ingress>.<cmd>" 主题重新发布到同一个流，供各 cmd 下游消费者分别处理。
func HandleAllNatsMessage(ctx context.Context, conf nats.NatsConf, js jetstream.JetStream, msg jetstream.Msg) {
	cmd := extractCmd(msg.Data())
	if cmd == "" {
		// 没有 cmd 无法路由：丢弃并告警，避免 Nak 造成永久重投的死循环。
		log.Printf("[NATS] 跳过无 cmd 的消息: %s", string(msg.Data()))
		_ = msg.Ack()
		return
	}

	// 发布主题必须用 "." 分隔，与流过滤器 (ingress+".>") 及下游 FilterSubject 精确匹配。
	// 切勿对主题使用 sanitizeName：它会把 "." 换成 "_"，导致主题匹配不到任何流，报 "no response from stream"。
	newSubject := conf.Subject + "." + cmd
	if _, err := js.Publish(ctx, newSubject, msg.Data()); err != nil {
		log.Printf("[NATS] 分发失败 subject=%s: %v", newSubject, err)
		_ = msg.Nak()
		return
	}

	_ = msg.Ack()
	log.Printf("[NATS] 已路由至 %s", newSubject)
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

// HandleNatsMessage 是各 cmd 下游消费者的回调：反序列化后执行设备连接逻辑。
func HandleNatsMessage(msg jetstream.Msg, svcCtx *svc.ServiceContext) {
	req := &pb.DeviceConnectReq{}
	if err := protojson.Unmarshal(msg.Data(), req); err != nil {
		log.Printf("[NATS] failed to unmarshal message: %v", err)
		return
	}
	if connId := msg.Headers().Get("ConnId"); connId != "" {
		req.ConnId = connId
	}
	if serviceName := msg.Headers().Get("ServiceName"); serviceName != "" {
		req.ServiceName = serviceName
	}
	ctx := context.Background()
	l := logic.NewDeviceConnectLogic(ctx, svcCtx)
	resp, err := l.DeviceConnect(req).Iotdevice(req)
	if err != nil {
		log.Printf("[NATS] handle device connect error: %v", err)
		return
	}
	if resp != nil {
		log.Printf("[NATS] device connect result: %s", resp.Message)
	}
}
