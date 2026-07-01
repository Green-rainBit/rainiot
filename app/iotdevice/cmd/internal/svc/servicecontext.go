// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/model"
	"rainiot/pkg/configcli/nacos"
	"rainiot/pkg/openconfig"

	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config       config.Config
	DeviceModel  model.DeviceModel
	Redis        *goredis.ClusterClient
	NatsConsumer *NatsConsumer
}

func NewServiceContext(c config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	nacosCli, err := nacos.NewNacosClient(nacosconfig)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	nacosCli.InitNacosConfig(nacosconfig.DataId, nacosconfig.Group, func(namespace, group, dataId, data string) {
		json.Unmarshal([]byte(data), &c)
	})
	nacosCli.InitNacosRegisterInstance(nacosconfig, c.RestConf)
	nacosCli.InitNacosRegisterInstanceGrpc(nacosconfig, c.Rpc)
	driverName := strings.TrimSpace(c.DriverName)
	if driverName == "" {
		driverName = "postgres"
	}
	conn := sqlx.NewSqlConn(driverName, c.DataSource)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})

	svcCtx := &ServiceContext{
		Config:      c,
		DeviceModel: model.NewDeviceModel(conn),
		Redis:       client,
	}

	// NATS 消费者在 main.go 中通过 NewNatsConsumer 创建并注入 handler，
	// 以避免 svc → logic → svc 的循环导入。此处仅持有引用用于 defer Close。
	// 如果配置了 NATS，将在 main.go 中初始化。

	return svcCtx
}
