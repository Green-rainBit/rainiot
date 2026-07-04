package natshandler

import (
	"context"
	"log"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const defaultIngressSubject = "raw_ingress"

// ingressSubject 返回入口主题：优先使用配置的 Nats.Subject，否则回退到 raw_ingress。
func ingressSubject(serverCtx *svc.ServiceContext) string {
	if s := strings.TrimSpace(serverCtx.Config.Nats.Subject); s != "" {
		return s
	}
	return defaultIngressSubject
}

// sanitizeName 把含 "." 的主题/命令转成合法的 JetStream 流名 / durable 名。
// 仅用于 StreamConfig.Name 与 ConsumerConfig.Durable；
// 绝不能用于发布主题或 FilterSubject——那些必须保留 "."。
func sanitizeName(parts ...string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.Join(parts, "_"), ".", "_"))
}

// StartRouter 启动分流器：创建入口流、路由消费者（提取 cmd 并分发到 <ingress>.<cmd>），
// 以及各 cmd 的下游消费者。
func StartRouter(serverCtx *svc.ServiceContext) {
	ctx := context.Background()
	js := serverCtx.NatsJetStream
	ingress := ingressSubject(serverCtx)

	// 入口流同时捕获原始消息 (ingress) 与分发后的子命令消息 (ingress.<cmd>)。
	// 通配符必须用 ".>"；"_>" 会被当成字面量，匹配不到任何子主题。
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      sanitizeName(ingress),
		Subjects:  []string{strings.ToLower(ingress), strings.ToLower(ingress) + ".>"},
		Retention: jetstream.WorkQueuePolicy, // 工作队列
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		log.Printf("[NATS] 创建流失败: %v", err)
		return
	}

	// 路由消费者：只消费原始入口消息，提取 cmd 后分发到 <ingress>.<cmd>。
	// 必须带 FilterSubject，否则会再次消费自己分发出去的消息，形成死循环。
	router, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig(sanitizeName(ingress, "router"), ingress))
	if err != nil {
		log.Printf("[NATS] 创建路由消费者失败: %v", err)
		return
	}
	router.Consume(func(msg jetstream.Msg) {
		HandleAllNatsMessage(ctx, serverCtx.Config.Nats, js, msg)
	})

	if err := NewConsumer(ctx, serverCtx, stream); err != nil {
		log.Printf("[NATS] 创建命令消费者失败: %v", err)
	}
}

// deviceCmds 是支持的设备命令：每个命令对应一个 <ingress>.<cmd> 下游消费者。
var deviceCmds = []string{
	"connect", "disconnect", "login", "logout",
	"error", "info", "push", "reload",
}

// NewConsumer 为每个 cmd 创建并启动一个下游消费者，订阅 <ingress>.<cmd>。
func NewConsumer(ctx context.Context, serverCtx *svc.ServiceContext, stream jetstream.Stream) error {
	ingress := ingressSubject(serverCtx)
	for _, cmd := range deviceCmds {
		subject := ingress + "." + cmd
		consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig(sanitizeName(ingress, cmd), subject))
		if err != nil {
			log.Printf("[NATS] 创建消费者失败 cmd=%s: %v", cmd, err)
			return err
		}
		log.Printf("[NATS] 消费者就绪 %s -> %s", consumer.CachedInfo().Name, subject)
		consumer.Consume(func(msg jetstream.Msg) {
			HandleNatsMessage(msg, serverCtx)
			_ = msg.Ack()
		})
	}

	return nil
}

// consumerConfig 构造一个 AckExplicit 消费者配置：durable 名用 sanitizeName 清洗（不含 "."），
// FilterSubject 保留 "." 以匹配主题通配规则。
func consumerConfig(durable, filterSubject string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:           durable,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           60 * time.Second,
		FilterSubject:     filterSubject,
		MaxAckPending:     100,
		InactiveThreshold: 10 * time.Second,
	}
}
