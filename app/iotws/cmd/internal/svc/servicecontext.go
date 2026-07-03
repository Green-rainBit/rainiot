package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/ws"
	"rainiot/pkg/alarm"
	"rainiot/pkg/configcli"
	"rainiot/pkg/devicecli"
	"rainiot/pkg/devicecli/grpc/instances"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/registry"
	"rainiot/pkg/util"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/resolver"
)

type ServiceContext struct {
	Config     *config.Config
	Redis      *redis.ClusterClient
	Connection ws.Connection
	DeviceCli  devicecli.DeviceCli
	Upgrader   *gws.Upgrader
	gateway    *ws.Gateway
}

func NewServiceContext(c *config.Config, openConfig openconfig.OpenConfig) *ServiceContext {
	cf, ok, err := configcli.SyncConfig(openConfig, func(data string) {
		unlock := c.Lock()
		json.Unmarshal([]byte(data), c)
		unlock()
	})
	if err != nil {
		log.Fatalf("init sync config err: %v", err)
	}
	if ok {
		json.Unmarshal([]byte(cf), c)
	}
	registrycli, ok, err := registry.NewRegistry(openConfig)
	if err != nil {
		log.Fatalf("init registry err: %v", err)
	}
	if ok {
		registrycli.InitRegisterInstance(c.RestConf)
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	configcli := c
	if registrycli != nil && c.Rpc.Model == "nacos" {
		if c.Rpc.RpcClientConf.Target == "" {
			c.Rpc.RpcClientConf.Target = openConfig.RegistryConfig.BuildConfigUrl("iotdevice.grpc")
		}
	} else if c.Rpc.Model == "instances" {
		resolver.Register(instances.NewBuilder(func() []string {
			return configcli.GetHealthyInstances("iotdevice.grpc")
		}))
		c.Rpc.RpcClientConf.Target = "instances://iotdevice.grpc"
	}

	serviceName, _, _ := util.GetRegistryParameters(c.RestConf)
	connection := ws.NewConnection()
	gateway := ws.NewGatewayr(serviceName, connection, client)

	devCli := devicecli.NewDeviceCli(c.TransportModel, serviceName, configcli, c.Rpc.RpcClientConf, &c.Nats, alarm.New(c.AlarmConfig))

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: connection,
		DeviceCli:  devCli,
		gateway:    gateway,
		Upgrader: gws.NewUpgrader(gateway, &gws.ServerOption{
			Recovery: func(logger gws.Logger) {
				func() {
					if r := recover(); r != nil {
						logger.Error(r)
					}
				}()
			},
			NewSession: func() gws.SessionStorage {
				return gws.NewConcurrentMap[string, any](1)
			},
			ReadBufferSize:      512,
			WriteBufferSize:     512,
			ParallelEnabled:     false,
			PermessageDeflate:   gws.PermessageDeflate{Enabled: false},
			CheckUtf8Enabled:    false,
			ReadMaxPayloadSize:  4096,
			WriteMaxPayloadSize: 4096,
		}),
	}
}

func (s *ServiceContext) WireWsFn(fn func(connId string, message []byte) ([]byte, bool, error)) {
	s.gateway.Fn = fn
}

// CloseWs 优雅关闭所有 WebSocket 连接,委托给 gateway。
func (s *ServiceContext) CloseWs() {
	s.gateway.CloseAll()
}
