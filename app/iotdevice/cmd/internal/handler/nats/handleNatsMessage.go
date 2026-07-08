package natshandler

import (
	"bytes"

	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"

	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/encoding/protojson"
)

// HandleAllNatsMessage 是路由消费者回调：从原始入口消息里提取 cmd，
// 直接路由到对应的 logic 处理，避免重新发布到 NATS 带来的二次 JetStream 开销。
func HandleAllNatsMessage(svcCtx *svc.ServiceContext, msg *message.Message) error {
	cmd := extractCmd(msg.Payload)
	if cmd == "" {
		return nil
	}
	logicMap := map[string]struct{}{}
	logicMap["login"] = struct{}{}
	if _, ok := logicMap[cmd]; !ok {
		return nil
	}

	// 直接调用 logic，不再重新发布到 NATS（消除 ensureStream + PublishMsg 二次开销）
	return routeToLogic(cmd, svcCtx, msg)
}

// routeToLogic 将消息直接路由到对应的 logic 处理，替代原来的 NATS 二次发布模式。
func routeToLogic(cmd string, svcCtx *svc.ServiceContext, msg *message.Message) error {
	req := &pb.DeviceConnectReq{}
	if err := protojson.Unmarshal(msg.Payload, req); err != nil {
		return err
	}
	if connId := msg.Metadata.Get("ConnId"); connId != "" {
		req.ConnId = connId
	}
	if serviceName := msg.Metadata.Get("ServiceName"); serviceName != "" {
		req.ServiceName = serviceName
	}
	_, err := logic.NewIotdeviceLogic(msg.Context(), req.Cmd, svcCtx).Iotdevice(req)
	return err
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
