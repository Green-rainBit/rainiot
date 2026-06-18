package nacos

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/util"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	_ "github.com/zeromicro/zero-contrib/zrpc/registry/nacos"
)

type nacosClient struct {
	config    openconfig.NacosConfig
	confCli   config_client.IConfigClient
	namingCli naming_client.INamingClient

	mu         sync.RWMutex
	instances  map[string][]string // 格式：ip:port
	lastUpdate time.Time
}

func NewNacosClient(config openconfig.NacosConfig) (*nacosClient, error) {
	if config.Model != "nacos" || len(config.IpAddress) == 0 {
		return nil, nil
	}
	nacosConfigs := []constant.ServerConfig{}
	for _, ipAddress := range config.IpAddress {
		nacosConfigs = append(nacosConfigs, *constant.NewServerConfig(ipAddress, config.Port))
	}
	nacosClientParam := vo.NacosClientParam{
		ClientConfig: constant.NewClientConfig(
			constant.WithNamespaceId(config.NamespaceId), //当namespace是public时，此处填空字符串。
			constant.WithTimeoutMs(5000),
			constant.WithNotLoadCacheAtStart(true),
			constant.WithLogDir("tmp/nacos/log"),
			constant.WithCacheDir("tmp/nacos/cache"),
			constant.WithLogLevel("debug"),
			constant.WithUsername(config.Username),
			constant.WithPassword(config.Password),
			constant.WithAccessKey(config.AccessKey),
			constant.WithSecretKey(config.SecretKey),
			constant.WithRegionId(config.RegionId),
		),
		ServerConfigs: nacosConfigs,
	}

	configClient, err := clients.NewConfigClient(
		nacosClientParam,
	)
	if err != nil {
		return nil, err
	}
	namingClient, err := clients.NewNamingClient(
		nacosClientParam,
	)
	if err != nil {
		return nil, err
	}

	return &nacosClient{
		config:    config,
		confCli:   configClient,
		namingCli: namingClient,
	}, nil

}

func (l *nacosClient) InitNacosConfig(dataId, group string, onChange func(namespace, group, dataId, data string)) error {
	err := l.confCli.ListenConfig(vo.ConfigParam{
		DataId:   dataId,
		Group:    group,
		OnChange: onChange,
	})
	if err != nil {
		return err
	}
	return nil
}

func (l *nacosClient) InitNacosRegisterInstance(config openconfig.NacosConfig, c rest.RestConf) error {
	serviceName, ip, portStr := util.GetRegistryParameters(c)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SERVICE_PORT: %w", err)
	}
	_, err = l.namingCli.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: config.DataId,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	go handleShutdown(l.namingCli, serviceName, ip, port)
	return nil
}

func (l *nacosClient) GetSeverCli(serviceName, groupName string) error {

	instances, err := l.namingCli.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,             // 默认值DEFAULT_GROUP
		Clusters:    []string{"cluster-a"}, // 默认值DEFAULT
	})
	if err != nil {
		return nil
	}
	addrs := make([]string, 0, len(instances))
	for _, inst := range instances {
		addrs = append(addrs, fmt.Sprintf("%s:%d", inst.Ip, inst.Port))
	}
	l.mu.Lock()
	l.instances[serviceName+":"+groupName] = addrs
	l.lastUpdate = time.Now()
	l.mu.Unlock()
	return nil
}

// autoRefresh 定期刷新
func (l *nacosClient) autoRefresh() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		for key, _ := range l.instances {
			arr := strings.Split(key, ":")
			if len(arr) != 2 {
				continue
			}
			_ = l.GetSeverCli(arr[0], arr[1]) // 忽略错误，保留旧列表
		}

	}
}

// GetHealthyInstances 返回当前健康的实例列表（副本）
func (l *nacosClient) GetHealthyInstances(serviceName, groupName string) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if _, ok := l.instances[serviceName+":"+groupName]; !ok {
		_ = l.GetSeverCli(serviceName, groupName) // 首次获取实例列表
	}
	out := make([]string, len(l.instances[serviceName+":"+groupName]))
	copy(out, l.instances[serviceName+":"+groupName])
	return out
}

func handleShutdown(namingClient naming_client.INamingClient, serviceName, ip string, port uint64) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	log.Println("\n[INFO] Shutdown signal received, deregistering...")
	_, err := namingClient.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		Ephemeral:   true,
	})
	if err != nil {
		log.Printf("[ERROR] Deregister failed: %v\n", err)
	} else {
		log.Println("[INFO] Deregistered successfully.")
	}
	os.Exit(0)
}

func (l *nacosClient) SetGrpcConfig(rpcClientConf *zrpc.RpcClientConf) error {
	if l.config.Model != "nacos" || len(l.config.IpAddress) == 0 {
		return nil
	}
	rpcClientConf.Target = l.config.BuildConfigUrl()
	return nil
}
