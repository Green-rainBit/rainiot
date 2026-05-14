// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"encoding/json"
	"log"
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
	DeviceCli  devicecli.DeviceCli
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
	nacosCli.InitNacosConfig(nacosconfig.DataId, nacosconfig.NamespaceId, func(namespace, group, dataId, data string) {
		json.Unmarshal([]byte(data), &c)
	})
	nacosCli.InitNacosRegisterInstance(nacosconfig, c.RestConf)

	deviceCli := devicecli.NewDeviceCli(c.Mode, func() (string, uint64, error) {
		return nacosCli.GetSeverCli(c.DeviceServer, nacosconfig.Group)
	}, c.DviceHost, c.DevicePort)

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: NewConnection(),
		DeviceCli:  deviceCli,
	}
}
