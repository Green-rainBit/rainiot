// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"log"
	"net/http"
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/pkg/devicecli"
	"rainiot/pkg/nacos"
	"rainiot/pkg/openconfig"

	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config     config.Config
	Redis      *redis.ClusterClient
	Connection *connection
	DeviceCli  func() (http.Client, error)
}

func NewServiceContext(c config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	nacosCli, err := nacos.NewNacosClient(nacosconfig)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	deviceCli := devicecli.NewDeviceCli(c.Mode, func() (string, uint64, error) {
		return nacosCli.GetSeverCli(c.DeviceServer, nacosconfig.Group)
	}, c.DviceHost, c.DevicePort)

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: NewConnection(c),
		DeviceCli:  deviceCli,
	}
}
