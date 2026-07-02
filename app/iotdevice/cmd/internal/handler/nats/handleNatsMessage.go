package natshandler

import (
	"context"
	"log"
	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"

	natsio "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

func HandleNatsMessage(msg *natsio.Msg, svcCtx *svc.ServiceContext) {
	req := &pb.DeviceConnectReq{}
	if err := protojson.Unmarshal(msg.Data, req); err != nil {
		log.Printf("[NATS] failed to unmarshal message: %v", err)
		return
	}
	if connId := msg.Header.Get("ConnId"); connId != "" {
		req.ConnId = connId
	}
	if serviceName := msg.Header.Get("ServiceName"); serviceName != "" {
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
