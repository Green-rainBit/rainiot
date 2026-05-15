package main

import (
	"context"
	"flag"

	"rainiot/app/iotcron/cmd/internal/config"
	"rainiot/app/iotcron/cmd/internal/logic"
	"rainiot/app/iotcron/cmd/internal/svc"
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
	logx.MustSetup(c.Log)
	defer logx.Close()

	var nacosconfig openconfig.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	svcCtx := svc.NewServiceContext(c)
	mux := logic.NewCronJob(context.Background(), svcCtx).Register()
	mux.Run()
}
