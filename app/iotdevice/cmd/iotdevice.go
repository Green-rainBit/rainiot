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

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
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

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
