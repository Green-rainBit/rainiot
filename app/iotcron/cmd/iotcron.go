package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"rainiot/app/iotcron/cmd/internal/config"
	"rainiot/app/iotcron/cmd/internal/logic"
	"rainiot/app/iotcron/cmd/internal/svc"
	plog "rainiot/pkg/log"
	"rainiot/pkg/openconfig"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String("f", "etc/iotcron.json", "the config file")
var configNacosFile = flag.String("nacos", "etc/iotcron-nacos.json", "the nacos config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	var nacosconfig openconfig.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	// 统一日志配置：根据 Loki.Mode 自动选择直写/桥接/双写
	logWriter, err := plog.Setup(c.Log, c.Loki, nil)
	if err != nil {
		logx.Errorf("log setup: %v", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
	}

	svcCtx := svc.NewServiceContext(c)
	mux := logic.NewCronJob(context.Background(), svcCtx).Register(logWriter)
	mux.Start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	<-mux.Stop().Done() // 等待在途任务
	_ = svcCtx.AsynqClient.Close()
	_ = svcCtx.Redis.Close()

}
