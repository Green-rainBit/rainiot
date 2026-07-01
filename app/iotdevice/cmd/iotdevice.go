package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"rainiot/pkg/openconfig"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/cmd/internal/handler"
	"rainiot/app/iotdevice/cmd/internal/logic"
	"rainiot/app/iotdevice/cmd/internal/svc"
	"rainiot/pkg/devicecli/grpc/pb"
	plog "rainiot/pkg/log"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"

	servergrpc "rainiot/app/iotdevice/cmd/internal/server"

	natsio "github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

var configFile = flag.String("f", "etc/iotdevice-api.json", "the config file")
var configNacosFile = flag.String("nacos", "etc/iotdevice-nacos.json", "the nacos config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	var nacosconfig openconfig.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	// 统一日志配置：根据 Loki.Mode 自动选择直写/桥接/双写
	logWriter, err := plog.Setup(c.Log, c.Loki, c.Nats.Urls)
	if err != nil {
		log.Fatalf("log setup: %v", err)
	}
	if logWriter != nil {
		defer logWriter.Close()
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c, nacosconfig)

	if len(c.Nats.Urls) > 0 {
		natsConsumer, err := svc.NewNatsConsumer(c.Nats, func(msg *natsio.Msg) {
			handleNatsMessage(msg, ctx)
		})
		if err != nil {
			log.Fatalf("init nats consumer err: %v", err)
		}
		ctx.NatsConsumer = natsConsumer
		defer ctx.NatsConsumer.Close()
	}

	handler.RegisterHandlers(server, ctx)

	s := zrpc.MustNewServer(c.Rpc, func(grpcServer *grpc.Server) {
		pb.RegisterIotdeviceServer(grpcServer, servergrpc.NewIotdeviceServer(ctx))
		if c.Rpc.Mode == service.DevMode || c.Rpc.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	go func() { s.Start() }()

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}

func handleNatsMessage(msg *natsio.Msg, svcCtx *svc.ServiceContext) {
	req := &pb.DeviceConnectReq{}
	if err := protojson.Unmarshal(msg.Data, req); err != nil {
		log.Printf("[NATS] failed to unmarshal message: %v", err)
		return
	}
	if connId := msg.Header.Get("ConnId"); connId != "" {
		req.ConnId = connId
	}
	if serviceName := msg.Header.Get("ServiceName"); serviceName != "" {
		req.ServiceName = serviceName
	}
	ctx := context.Background()
	l := logic.NewDeviceConnectLogic(ctx, svcCtx)
	resp, err := l.DeviceConnect(req).Iotdevice(req)
	if err != nil {
		log.Printf("[NATS] handle device connect error: %v", err)
		return
	}
	if resp != nil {
		log.Printf("[NATS] device connect result: %s", resp.Message)
	}
}
