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
)

type NacosClient interface {
}

type nacosClient struct {
	config    openconfig.NacosConfig
	confCli   config_client.IConfigClient
	namingCli naming_client.INamingClient
	instances sync.Map
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
	nc := &nacosClient{
		config:    config,
		confCli:   configClient,
		namingCli: namingClient,
	}
	util.Go(nc.autoRefresh)

	return nc, nil

}

func (l *nacosClient) InitNacosConfig(dataId, group string, onChange func(namespace, group, dataId, data string)) (string, error) {
	configstring, err := l.confCli.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		return "", err
	}
	err = l.confCli.ListenConfig(vo.ConfigParam{
		DataId:   dataId,
		Group:    group,
		OnChange: onChange,
	})
	if err != nil {
		return "", err
	}
	return configstring, nil
}

func (l *nacosClient) InitNacosRegisterInstanceGrpc(config openconfig.NacosConfig, c zrpc.RpcServerConf) error {
	if l.config.Model != "nacos" || len(l.config.IpAddress) == 0 {
		return nil
	}
	ip, portStr := util.GetGrpcRegistryParameters(c.Name, c.ListenOn)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid gRPC port: %w", err)
	}
	_, err = l.namingCli.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: c.Name,
		GroupName:   l.groupOrDefault(config.Group),
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register gRPC service: %w", err)
	}
	log.Printf("[Nacos] registered gRPC instance: service=%s group=%s ip=%s port=%d", c.Name, l.groupOrDefault(config.Group), ip, port)

	go handleShutdown(l.namingCli, c.Name, ip, port)
	return nil
}

func (l *nacosClient) InitNacosRegisterInstance(config openconfig.NacosConfig, c rest.RestConf) error {
	_, ip, portStr := util.GetRegistryParameters(c)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SERVICE_PORT: %w", err)
	}
	_, err = l.namingCli.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: config.DataId,
		GroupName:   l.groupOrDefault(config.Group),
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	log.Printf("[Nacos] registered instance: service=%s group=%s ip=%s port=%d", config.DataId, l.groupOrDefault(config.Group), ip, port)

	go handleShutdown(l.namingCli, config.DataId, ip, port)
	return nil
}

func (l *nacosClient) groupOrDefault(groupName string) string {
	if groupName == "" {
		return "DEFAULT_GROUP"
	}
	return groupName
}

func (l *nacosClient) GetSeverCli(serviceName, groupName string) error {
	groupName = l.groupOrDefault(groupName)
	log.Printf("[Nacos] querying instances: service=%s group=%s", serviceName, groupName)
	instances, err := l.namingCli.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		HealthyOnly: true,
	})
	if err != nil {
		log.Printf("[Nacos] SelectInstances error: service=%s group=%s err=%v", serviceName, groupName, err)
		return err
	}
	addrs := make([]string, 0, len(instances))
	for _, inst := range instances {
		log.Printf("[Nacos] found instance: %s:%d healthy=%v enable=%v weight=%f", inst.Ip, inst.Port, inst.Healthy, inst.Enable, inst.Weight)
		addrs = append(addrs, fmt.Sprintf("%s:%d", inst.Ip, inst.Port))
	}
	log.Printf("[Nacos] total instances found: %d", len(addrs))
	l.instances.Store(serviceName+":"+groupName, addrs)
	return nil
}

// autoRefresh 定期刷新
func (l *nacosClient) autoRefresh() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		l.instances.Range(func(key, value any) bool {
			k := key.(string)
			arr := strings.Split(k, ":")
			if len(arr) != 2 {
				return true
			}
			err := l.GetSeverCli(arr[0], arr[1]) // 忽略错误，保留旧列表
			if err != nil {
				log.Println(err)
			}
			return true
		})
	}
}

// GetHealthyInstances 返回当前健康的实例列表（副本）
func (l *nacosClient) GetHealthyInstances(serviceName string) []string {
	key := serviceName + ":" + l.config.Group
	v, ok := l.instances.Load(key)
	if !ok {
		if err := l.GetSeverCli(serviceName, l.config.Group); err != nil {
			log.Println(err)
			return nil
		}
		v, ok = l.instances.Load(key)
		if !ok {
			return nil
		}
	}
	addrs := v.([]string)
	out := make([]string, len(addrs))
	copy(out, addrs)
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
		log.Printf("[ERROR] Deregister failed: %v\n", serviceName, err)
	} else {
		log.Println("[INFO] Deregistered successfully.", serviceName)
	}
	os.Exit(0)
}

func (l *nacosClient) SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf) {
	if l.config.Model != "nacos" || len(l.config.IpAddress) == 0 {
		return
	}
	rpcClientConf.Target = l.config.BuildConfigUrl(serviceName)
}

func (l *nacosClient) GetNacosClient() naming_client.INamingClient {
	return l.namingCli
}
