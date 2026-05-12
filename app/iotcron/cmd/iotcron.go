package cmd

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
var configNacosFile = flag.String("nacos", "etc/iotdevice-nacos.json", "the nacos config file")

func main() {

	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	logx.MustSetup(c.Log)
	defer logx.Close()

	var nacosconfig openconfig.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	// srv := asynq.NewServer(
	// 	asynq.RedisClientOpt{Addr: "localhost:6379"},
	// 	asynq.Config{Concurrency: 10},
	// )
	svcCtx := svc.NewServiceContext(c)
	mux := logic.NewCronJob(context.Background(), svcCtx).Register()
	mux.Run()
	// 不需要适配，因为ServeMux实现了Handler接口
	// if err := srv.Run(mux); err != nil {
	// 	log.Fatal(err)
	// }

}
