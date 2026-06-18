// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"encoding/json"
	"log"
	"strings"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/ws"
	"rainiot/pkg/devicecli"
	"rainiot/pkg/nacos"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/util"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
	_ "github.com/zeromicro/zero-contrib/zrpc/registry/nacos"
)

type ServiceContext struct {
	Config     config.Config
	Redis      *redis.ClusterClient
	Connection ws.Connection
	DeviceCli  devicecli.DeviceCli
	Upgrader   *gws.Upgrader
	gateway    *ws.Gateway
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
	serviceName, _, _ := util.GetRegistryParameters(c.RestConf)
	connection := ws.NewConnection()
	gateway := ws.NewGatewayr(serviceName, connection, client)

	nacosCli.InitNacosConfig(nacosconfig.DataId, nacosconfig.NamespaceId, func(namespace, group, dataId, data string) {
		json.Unmarshal([]byte(data), &c)
	})
	nacosCli.InitNacosRegisterInstance(nacosconfig, c.RestConf)
	nacosCli.SetGrpcConfig(&c.RpcClientConf, c.DeviceServer)

	return &ServiceContext{
		Config:     c,
		Redis:      client,
		Connection: connection,
		DeviceCli: devicecli.NewDeviceCli("grpc", serviceName, func(deviceServiceName string) []string {
			return nacosCli.GetHealthyInstances(deviceServiceName, nacosconfig.Group)
		}, c.RpcClientConf),
		gateway: gateway,
		Upgrader: gws.NewUpgrader(gateway, &gws.ServerOption{
			// ParallelEnabled:   true,                                 // 开启并行消息处理
			Recovery: func(logger gws.Logger) {
				logger.Error("panic:", recover())
			}, // 开启异常恢复
			// PermessageDeflate: gws.PermessageDeflate{Enabled: true}, // 开启压缩

			NewSession: func() gws.SessionStorage {
				return gws.NewConcurrentMap[string, any](1)
			},

			ReadBufferSize:      512,                                   // 读缓冲区从4KB降到512B，10万连接可节省约700MB内存
			WriteBufferSize:     512,                                   // 写缓冲区同样降低
			ParallelEnabled:     false,                                 // 1000 QPS 完全不需要并行处理，可避免 goroutine 数量过多
			PermessageDeflate:   gws.PermessageDeflate{Enabled: false}, // 开启压缩
			CheckUtf8Enabled:    false,                                 // 如果消息确定是UTF-8或二进制，可关闭校验以节省CPU
			ReadMaxPayloadSize:  4096,                                  // 限制最大消息体，防止恶意大包攻击
			WriteMaxPayloadSize: 4096,
		}),
	}
}

func (s *ServiceContext) WireWsFn(fn func(connId string, message []byte) ([]byte, bool, error)) {
	s.gateway.Fn = fn
}
