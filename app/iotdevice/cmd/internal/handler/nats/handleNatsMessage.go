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

func HandleAllNatsMessage(ctx context.Context, conf nats.NatsConf, js jetstream.JetStream, msg jetstream.Msg) {
	newSubject := conf.Subject + "." + extractCmd(msg.Data())
	if _, err := js.Publish(ctx, newSubject, msg.Data()); err != nil {
		// 分发失败
		msg.Nak()
		return
	}

	// 6. 全部成功，手动确认原始消息（从 raw.ingress 中移除）
	msg.Ack()
	log.Printf("消息已路由至: %s", newSubject)
}

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
