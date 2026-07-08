package natshandler

import (
	"context"
	"log"

	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"

	"google.golang.org/protobuf/encoding/protojson"
)

func StartRouter(svcCtx *svc.ServiceContext) {
	ctx := context.Background()
	consumer, err := svcCtx.Queue.Subscribe(ctx, "iot_device")
	if err != nil {
		log.Printf("[NATS] 创建消费者失败: %v", err)
		return
	}
	log.Printf("[NATS] 创建消费者成功")
	go func() {
		for msg := range consumer {
			err = HandleAllNatsMessage(svcCtx, msg)
			if err != nil {
				log.Printf("[NATS] 处理消息失败: %v", err)
				msg.Nack()
			}
			msg.Ack()
		}
	}()
	logicMap := map[string]func(ctx context.Context, cmd string, svcCtx *svc.ServiceContext) logic.IotdeviceLogic{}
	logicMap["login"] = logic.NewIotdeviceLogic
	for key, fn := range logicMap {
		consumer, err := svcCtx.Queue.Subscribe(ctx, "iot_device"+"_"+key)
		if err != nil {
			log.Printf("[NATS] 创建消费者失败: %v", err)
			return
		}
		log.Printf("[NATS] 创建消费者成功")
		go func() {
			for msg := range consumer {
				req := &pb.DeviceConnectReq{}
				if err := protojson.Unmarshal(msg.Payload, req); err != nil {
					log.Printf("[NATS] failed to unmarshal message: %v", err)
					return
				}
				if connId := msg.Metadata.Get("ConnId"); connId != "" {
					req.ConnId = connId
				}
				if serviceName := msg.Metadata.Get("ServiceName"); serviceName != "" {
					req.ServiceName = serviceName
				}
				_, err = fn(msg.Context(), req.Cmd, svcCtx).Iotdevice(req)
				if err != nil {
					log.Printf("[NATS] 处理消息失败: %v", err)
				}
				msg.Ack()
			}
		}()

	}

}
