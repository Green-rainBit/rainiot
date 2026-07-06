package main

import (
	"flag"
	"fmt"
	"log"

	"rainiot/pkg/openconfig"
	"rainiot/pkg/util"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/cmd/internal/handler"
	natshandler "rainiot/app/iotdevice/cmd/internal/handler/nats"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"
	plog "rainiot/pkg/log"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"

	servergrpc "rainiot/app/iotdevice/cmd/internal/server"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/iotdevice-api.json", "the config file")
var configNacosFile = flag.String("nacos", "etc/iotdevice-nacos.json", "the nacos config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	var ocf openconfig.OpenConfig
	conf.MustLoad(*configNacosFile, &ocf)

	// 统一日志配置：根据 Loki.Mode 自动选择直写/桥接/双写
	logWriter, err := plog.Setup(c.Log, c.Loki, c.MQ.NATS.Addresses)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
	}

	ctx := svc.NewServiceContext(&c, ocf)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	handler.RegisterHandlers(server, ctx)
	natshandler.StartRouter(ctx)

	s := zrpc.MustNewServer(c.Rpc, func(grpcServer *grpc.Server) {
		pb.RegisterIotdeviceServer(grpcServer, servergrpc.NewIotdeviceServer(ctx))
		if c.Rpc.Mode == service.DevMode || c.Rpc.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	util.Go(func() { s.Start() })

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
