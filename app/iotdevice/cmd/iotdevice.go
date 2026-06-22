// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package main

import (
	"flag"
	"fmt"

	"rainiot/pkg/openconfig"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/cmd/internal/handler"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"

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

	var nacosconfig openconfig.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c, nacosconfig)
	handler.RegisterHandlers(server, ctx)

	s := zrpc.MustNewServer(c.Rpc, func(grpcServer *grpc.Server) {
		pb.RegisterIotdeviceServer(grpcServer, servergrpc.NewIotdeviceServer(ctx))

		if c.Rpc.Mode == service.DevMode || c.Rpc.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	go func() {
		s.Start()
	}()
	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
