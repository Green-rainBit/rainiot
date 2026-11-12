package svc

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"rainiot/app/iotws/cmd/internal/config"
	"rainiot/app/iotws/cmd/internal/ws"
	"rainiot/pkg/alarm"
	"rainiot/pkg/client/configcli"
	"rainiot/pkg/client/devicecli"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/registry"
	"rainiot/pkg/util"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/rest"
)

type ServiceContext struct {
	instanceId string
	Config     *config.Config
	Redis      *redis.ClusterClient
	Connection ws.Connection
	DeviceCli  devicecli.DeviceCli
	Upgrader   *gws.Upgrader
	gateway    *ws.Gateway
}

func NewServiceContext(c *config.Config, ocp openconfig.OpenConfig) *ServiceContext {
	cf, ok, err := configcli.SyncConfig(ocp, func(data string) {
		unlock := c.DeviCliConfig.Lock()
		json.Unmarshal([]byte(data), c)
		unlock()
	})
	if err != nil {
		log.Fatalf("init sync config err: %v", err)
	}
	if ok {
		json.Unmarshal([]byte(cf), c)
	}
	instanceId, err := intInstanceId(c.RestConf)
	if err != nil {
		log.Fatalf("init instanceId err: %v", err)
	}
	registrycli, ok, err := registry.NewRegistry(ocp)
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
	c.DeviCliConfig.BuildDeviceServiceUrl(ocp)

	connection := ws.NewConnection()
	gateway := ws.NewGatewayr(instanceId, connection, client, c)
	gateway.StartPing()
	devCli := devicecli.NewDeviceCli(c.TransportModel, instanceId, &c.DeviCliConfig, alarm.New(c.AlarmConfig))

	return &ServiceContext{
		instanceId: instanceId,
		Config:     c,
		Redis:      client,
		Connection: connection,
		DeviceCli:  devCli,
		gateway:    gateway,
		Upgrader: gws.NewUpgrader(gateway, &gws.ServerOption{
			Recovery: func(logger gws.Logger) {
				if r := recover(); r != nil {
					logger.Error("panic:", r, string(debug.Stack()))
				}
			},
			NewSession: func() gws.SessionStorage {
				return gws.NewConcurrentMap[string, any](1)
			},
			ReadBufferSize:      c.WsConf.ReadBufferSize,
			WriteBufferSize:     c.WsConf.WriteBufferSize,
			ParallelEnabled:     c.WsConf.ParallelEnabled,
			ParallelGolimit:     c.WsConf.ParallelGolimit,
			PermessageDeflate:   gws.PermessageDeflate{Enabled: false},
			CheckUtf8Enabled:    c.WsConf.CheckUtf8Enabled,
			ReadMaxPayloadSize:  c.WsConf.ReadMaxPayloadSize,
			WriteMaxPayloadSize: c.WsConf.WriteMaxPayloadSize,
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

func (s *ServiceContext) CleanupSocket(socket *gws.Conn) {
	s.gateway.OnClose(socket, nil)
}

func (s *ServiceContext) GetInstanceId() string {
	return s.instanceId
}

func intInstanceId(c rest.RestConf) (string, error) {
	hostPart := os.Getenv("POD_UID")
	if hostPart == "" {
		hostPart = os.Getenv("INSTANCE_HOSTNAME")
	}
	if hostPart == "" {
		h, err := os.Hostname()
		if err != nil {
			return "", fmt.Errorf("instance: get hostname: %w", err)
		}
		hostPart = h
	}

	// 格式：{service}-{env}-{hostname}-{pid}-{startup_ts}
	// startup_ts 用来区分同 hostname 重拉（K8s 重建、容器重启）
	id := fmt.Sprintf("%s-%s-%s-%d-%d",
		util.GetServiceName(c),
		c.Mode,
		hostPart,
		os.Getpid(),
		time.Now().Unix(),
	)
	return id, nil
}
