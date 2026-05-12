// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"fmt"
	"log"
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/pkg/nacos"
	"rainiot/pkg/openconfig"

	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config     config.Config
	Redis      *redis.ClusterClient
	Connection *connection
}

func NewServiceContext(c config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	nacos.InitNacosConfig(nacosconfig, func(namespace, group, dataId, data string) {
		fmt.Println("group:" + group + ", dataId:" + dataId + ", data:" + data)
	})

	err := nacos.InitNacosRegisterInstance(nacosconfig)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: NewConnection(c),
	}
}
