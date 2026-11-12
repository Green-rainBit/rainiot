package main

import (
	"context"
	"flag"
	"log"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/handler"
	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache/consts"
	plog "rainiot/pkg/log"
	"rainiot/pkg/openconfig"

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
	logWriter, err := plog.Setup(c.Log, c.Loki, c.Loki.Addresses)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	sctx := svc.NewServiceContext(&c, oconfig)

	l := logic.NewIotwsLogic(context.Background(), sctx.DeviceCli, sctx)
	sctx.WireWsFn(l.Iotws)

	handler.RegisterHandlers(server, sctx)

	cr := cron.New(cron.WithSeconds(), cron.WithChain(
		cron.Recover(logWriter),
		cron.SkipIfStillRunning(logWriter),
	))
	handler.RegisterCron(cr, sctx)
	cr.Start()

	mux := handler.Register(sctx)

	go func() {
		if err := sctx.Srv.Run(mux); err != nil {
			log.Fatalf("Server 运行失败: %v", err)
		}
	}()
	// 注册优雅关闭:收到 SIGTERM/SIGINT 时,关闭 ws 长连接并停止 cron 调度。
	// 该回调与 go-zero 的 HTTP 优雅关闭并发执行,共享默认 ~4.5s 宽限期。
	proc.AddShutdownListener(func() {
		sctx.CloseWs()
		cr.Stop()
		sctx.Srv.Shutdown()

		_ = sctx.Inspector.DeleteQueue(consts.WsBalanceQueue(sctx.GetInstanceId()), true)
		_ = sctx.Inspector.Close()
	})

	server.Start()
}
