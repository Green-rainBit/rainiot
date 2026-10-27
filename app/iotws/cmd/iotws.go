package main

import (
	"context"
	"flag"
	"log"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/handler"
	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"
	plog "rainiot/pkg/log"
	"rainiot/pkg/openconfig"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/iotws-api.json", "the config file")
var configNacosFile = flag.String("nacos", "etc/iotws-nacos.json", "the nacos config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	var oconfig openconfig.OpenConfig
	conf.MustLoad(*configNacosFile, &oconfig)

	// 统一日志配置：根据 Loki.Mode 自动选择直写/桥接/双写
	logWriter, err := plog.Setup(c.Log, c.Loki, c.MQ.NATS.Addresses)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(&c, oconfig)

	l := logic.NewIotwsLogic(context.Background(), ctx.DeviceCli, ctx)
	ctx.WireWsFn(l.Iotws)

	handler.RegisterHandlers(server, ctx)

	cr := cron.New(cron.WithSeconds(), cron.WithChain(
		cron.Recover(logWriter),
		cron.SkipIfStillRunning(logWriter),
	))
	handler.RegisterCron(cr, ctx)
	cr.Start()

	mux := handler.Register(ctx)

	queueName := cache.WsBalanceQueue(ctx.GetInstanceId())
	srv := asynq.NewServerFromRedisClient(
		ctx.Redis,
		asynq.Config{
			Concurrency: 10, // 并发 worker 数
			Queues:      map[string]int{queueName: 1},
		},
	)
	go func() {
		if err := srv.Run(mux); err != nil {
			log.Fatalf("Server 运行失败: %v", err)
		}
	}()
	// 注册优雅关闭:收到 SIGTERM/SIGINT 时,关闭 ws 长连接并停止 cron 调度。
	// 该回调与 go-zero 的 HTTP 优雅关闭并发执行,共享默认 ~4.5s 宽限期。
	proc.AddShutdownListener(func() {
		ctx.CloseWs()
		cr.Stop()
		srv.Shutdown()

		insp := asynq.NewInspectorFromRedisClient(ctx.Redis)
		_ = insp.DeleteQueue(cache.WsBalanceQueue(ctx.GetInstanceId()), true)
		_ = insp.Close()
	})

	server.Start()
}
