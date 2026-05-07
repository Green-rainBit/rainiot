// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package main

import (
	"flag"
	"fmt"
	"log"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/cmd/internal/handler"
	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/iotdevice-api.json", "the config file")
var configNacosFile = flag.String("f", "etc/nacos.json", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	var nacosconfig config.NacosConfig
	conf.MustLoad(*configNacosFile, &nacosconfig)

	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &nacosconfig.NacosAppClientConfig,
			ServerConfigs: nacosconfig.NacosSeverConfig,
		},
	)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	err = configClient.ListenConfig(vo.ConfigParam{
		DataId: "dataId",
		Group:  "group",
		OnChange: func(namespace, group, dataId, data string) {
			fmt.Println("group:" + group + ", dataId:" + dataId + ", data:" + data)
		},
	})
	if err != nil {
		log.Fatalf("listen nacos err: %v", err)
	}

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
