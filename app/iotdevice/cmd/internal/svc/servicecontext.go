// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/model"
	"rainiot/pkg/configcli"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/registry"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go/jetstream"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config        *config.Config
	DeviceModel   model.DeviceModel
	Redis         *goredis.ClusterClient
	NatsJetStream jetstream.JetStream
	Registry      registry.Registry
}

func NewServiceContext(c *config.Config, openConfig openconfig.OpenConfig) *ServiceContext {
	cf, ok, err := configcli.SyncConfig(openConfig, func(data string) {
		json.Unmarshal([]byte(data), c)
	})
	if err != nil {
		log.Fatalf("init sync config err: %v", err)
		return nil
	}
	if ok {
		err = json.Unmarshal([]byte(cf), c)
		if err != nil {
			log.Fatalf("init sync config err: %v", err)
			return nil
		}
	}
	registrycli, ok, err := registry.NewRegistry(openConfig)
	if err != nil {
		log.Fatalf("init registry err: %v", err)
	}
	if ok {
		registrycli.InitRegisterInstance(c.RestConf)
		registrycli.InitRegisterInstanceGrpc(c.Rpc)
	}

	driverName := strings.TrimSpace(c.DriverName)
	if driverName == "" {
		driverName = "postgres"
	}
	conn := sqlx.NewSqlConn(driverName, c.DataSource)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	natsJetStream, err := NewNatsJetStream(c.Nats)
	if err != nil {
		log.Fatalf("init nats err: %v", err)
	}
	svcCtx := &ServiceContext{
		Config:        c,
		DeviceModel:   model.NewDeviceModel(conn),
		Redis:         client,
		Registry:      registrycli,
		NatsJetStream: natsJetStream,
	}

	// NATS 消费者在 main.go 中通过 NewNatsConsumer 创建并注入 handler，
	// 以避免 svc → logic → svc 的循环导入。此处仅持有引用用于 defer Close。
	// 如果配置了 NATS，将在 main.go 中初始化。

	return svcCtx
}
