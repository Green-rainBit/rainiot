// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync/atomic"

	"rainiot/app/iotdevice/cmd/internal/config"
	"rainiot/app/iotdevice/model"
	"rainiot/pkg/configcli"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/queue"
	"rainiot/pkg/registry"
	"rainiot/pkg/wscli"

	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go/jetstream"
	goredis "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	config        atomic.Pointer[config.Config]
	DeviceModel   model.DeviceModel
	Redis         *goredis.ClusterClient
	NatsJetStream jetstream.JetStream
	Registry      registry.Registry
	Queue         queue.Queue
	wscli         *wscli.WsCli
}

func NewServiceContext(c *config.Config, openConfig openconfig.OpenConfig) *ServiceContext {
	svcCtx := &ServiceContext{}
	svcCtx.config.Store(c)
	cf, ok, err := configcli.SyncConfig(openConfig, func(data string) {
		next := *svcCtx.config.Load()
		if err := json.Unmarshal([]byte(data), &next); err != nil {
			logx.Errorf("invalid config: %v", err)
			return
		}
		svcCtx.config.Store(&next)
	})
	if err != nil {
		log.Fatalf("init sync config err: %v", err)
		return nil
	}
	if ok {
		next := *svcCtx.config.Load()
		if err := json.Unmarshal([]byte(cf), &next); err != nil {
			log.Fatalf("invalid config: %v", err)
			return nil
		}
		svcCtx.config.Store(&next)
	}
	registrycli, ok, err := registry.NewRegistry(openConfig)
	if err != nil {
		log.Fatalf("init registry err: %v", err)
	}
	if ok {
		registrycli.InitRegisterInstance(svcCtx.config.Load().RestConf)
		registrycli.InitRegisterInstanceGrpc(svcCtx.config.Load().Rpc)
	}

	driverName := strings.TrimSpace(svcCtx.config.Load().DriverName)
	if driverName == "" {
		driverName = "postgres"
	}
	conn := sqlx.NewSqlConn(driverName, svcCtx.config.Load().DataSource)
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    strings.Split(svcCtx.config.Load().CacheRedis.Host, ","),
		Password: svcCtx.config.Load().CacheRedis.Pass,
	})
	mq, err := queue.NewBrokerFromConfig(context.Background(), &svcCtx.config.Load().MQ)
	if err != nil {
		logx.Errorf("new broker from config: %v", err)
	}

	svcCtx.DeviceModel = model.NewDeviceModel(conn)
	svcCtx.Redis = client
	svcCtx.Registry = registrycli
	svcCtx.Queue = mq
	svcCtx.wscli = wscli.NewWsCli(configcli)

	// NATS 消费者在 main.go 中通过 NewNatsConsumer 创建并注入 handler，
	// 以避免 svc → logic → svc 的循环导入。此处仅持有引用用于 defer Close。
	// 如果配置了 NATS，将在 main.go 中初始化。

	return svcCtx
}

func (s *ServiceContext) Config() *config.Config {
	return s.config.Load()
}
