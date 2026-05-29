// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/model"
	"rainiot/pkg/nacos"
	"rainiot/pkg/openconfig"

	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config      config.Config
	DeviceModel model.DeviceModel
	Redis       *goredis.ClusterClient
}

func NewServiceContext(c config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	nacosCli, err := nacos.NewNacosClient(nacosconfig)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	nacosCli.InitNacosConfig(nacosconfig.DataId, nacosconfig.NamespaceId, func(namespace, group, dataId, data string) {
		json.Unmarshal([]byte(data), &c)
	})
	nacosCli.InitNacosRegisterInstance(nacosconfig, c.RestConf)
	
	driverName := strings.TrimSpace(c.DriverName)
	if driverName == "" {
		driverName = "postgres"
	}
	conn := sqlx.NewSqlConn(driverName, c.DataSource)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})

	return &ServiceContext{
		Config:      c,
		DeviceModel: model.NewDeviceModel(conn),
		Redis:       client,
	}
}
