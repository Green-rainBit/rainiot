package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/ws"
	"rainiot/pkg/alarm"
	configcli "rainiot/pkg/configcli"
	"rainiot/pkg/configcli/nacos"
	"rainiot/pkg/devicecli"
	"rainiot/pkg/devicecli/grpc/instances"
	"rainiot/pkg/devicecli/grpc/rpcn"
	"rainiot/pkg/openconfig"
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

func NewServiceContext(c *config.Config, nacosconfig openconfig.NacosConfig) *ServiceContext {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(c.CacheRedis.Host, ","),
		Password: c.CacheRedis.Pass,
	})
	nacosCli, err := nacos.NewNacosClient(nacosconfig)
	if err != nil {
		log.Fatalf("init nacos err: %v", err)
	}
	var reloadableCli devicecli.Reloadable

	var configcli configcli.ConfigCli = c
	if nacosCli != nil {
		data, err := nacosCli.InitNacosConfig(nacosconfig.DataId, nacosconfig.Group, func(namespace, group, dataId, data string) {
			unlock := c.Lock()
			json.Unmarshal([]byte(data), c)
			unlock()
			if reloadableCli != nil {
				reloadableCli.Reload(c.TransportModel)
			}
			switch c.TransportModel {
			case "nacos":
				resolver.Register(rpcn.NewBuilder(nacosCli.GetNacosClient()))
				c.Rpc.RpcClientConf.Target = nacosconfig.BuildConfigUrl("iotdevice.grpc")
			case "instances":
				resolver.Register(instances.NewBuilder(func() []string {
					return configcli.GetHealthyInstances("iotdevice.grpc")
				}))
				c.Rpc.RpcClientConf.Target = "instances://iotdevice.grpc"
			}
		})
		if err != nil {
			log.Fatalf("init nacos config err: %v", err)
		}
		json.Unmarshal([]byte(data), c)
		nacosCli.InitNacosRegisterInstance(nacosconfig, c.RestConf)
		configcli = nacosCli

	}

	if nacosCli != nil && c.Rpc.Model == "nacos" {
		resolver.Register(rpcn.NewBuilder(nacosCli.GetNacosClient()))
		c.Rpc.RpcClientConf.Target = nacosconfig.BuildConfigUrl("iotdevice.grpc")
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
	reloadableCli = devCli

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
					return
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
