package natshandler

import (
	"context"

	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"
	"rainiot/pkg/util"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/protobuf/encoding/protojson"
)

// cmdWorkerConfig 每个 cmd 的并发配置。
type cmdWorkerConfig struct {
	concurrency int // 最大并发处理数
}

// cmdWorkers 注册所有需要二次消费的 cmd。每个 cmd 通过独立 NATS subject 实现隔离。
// 慢 cmd 设小并发值防止占满资源，快 cmd 设大值保证高吞吐。
var cmdWorkers = map[string]cmdWorkerConfig{
	"login": {concurrency: 256},
	// "report":  {concurrency: 16},
	// "upgrade": {concurrency: 8},
}

// StartRouter 启动 NATS 消费者：
//   - consumer 1 (iot_device): 入口路由，go func per msg → HandleAllNatsMessage → 按 cmd 二次发布
//   - consumer 2 (iot_device_login): 并行消费，sema 控制并发 → protojson → logic.Iotdevice
//
// 二次分发保留 NATS 队列组的 cmd 隔离能力，但第二级消费者改为 sema + go func 并行处理。
//
// StartRouter 只负责启动消费者，启动完成后立即返回；消费循环在后台 goroutine 中运行。
// 调用方（main）必须在它返回后继续启动 HTTP/gRPC 服务，切勿在此阻塞。
func StartRouter(svcCtx *svc.ServiceContext) {
	if svcCtx.Queue == nil {
		logx.Info("[NATS] MQ 未配置或初始化失败，跳过消费者启动")
		return
	}
	logger := logx.WithContext(context.Background())
	subject := svcCtx.Config.MQ.Topic

	// ── Consumer 1: 入口路由 ──
	ingressConsumer, err := svcCtx.Queue.Subscribe(context.Background(), subject)
	if err != nil {
		logger.Errorf("[NATS] 入口消费者创建失败 subject=%s: %v", subject, err)
		return
	}
	logger.Infof("[NATS] 入口消费者启动: subject=%s", subject)

	util.Go(func() {
		for msg := range ingressConsumer {
			util.Go(func() {
				func(ms *message.Message) {
					if err := HandleAllNatsMessage(svcCtx, ms); err != nil {
						logger.Errorf("[NATS] 入口路由失败: %v", err)
						ms.Nack()
					}
				}(msg)
			})
		}
	})

	// ── Consumer 2: 按 cmd 的并行消费者 ──
	for cmd, cfg := range cmdWorkers {
		cmdSubject := subject + "." + cmd
		consumer, err := svcCtx.Queue.Subscribe(context.Background(), cmdSubject)
		if err != nil {
			logger.Errorf("[NATS] cmd=%s 消费者创建失败 subject=%s: %v", cmd, cmdSubject, err)
			continue
		}
		sema := make(chan struct{}, cfg.concurrency)
		logger.Infof("[NATS] cmd=%s 消费者启动: subject=%s concurrency=%d", cmd, cmdSubject, cfg.concurrency)

		util.Go(func() {
			for msg := range consumer {
				sema <- struct{}{}
				util.Go(func() {
					func(ms *message.Message) {
						defer func() { <-sema }()
						if err := processMessage(ms, svcCtx); err != nil {
							logger.Errorf("[NATS] cmd=%s 处理失败: %v", cmd, err)
							ms.Nack()
							return
						}
						ms.Ack()
					}(msg)
				})
			}
		})
	}
}

func processMessage(msg *message.Message, svcCtx *svc.ServiceContext) error {
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
