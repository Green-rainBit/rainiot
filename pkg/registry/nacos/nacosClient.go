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
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type nacosClient struct {
	logx.Logger
	config    openconfig.NacosConfig
	namingCli naming_client.INamingClient
	instances sync.Map
}

func NewNacosClient(config openconfig.NacosConfig) (*nacosClient, error) {
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
	namingClient, err := clients.NewNamingClient(
		nacosClientParam,
	)
	if err != nil {
		return nil, err
	}
	nc := &nacosClient{
		config:    config,
		namingCli: namingClient,
	}
	util.Go(nc.autoRefresh)

	return nc, nil

}

func (n *nacosClient) InitRegisterInstanceGrpc(c zrpc.RpcServerConf) error {
	ip, portStr := util.GetGrpcRegistryParameters(c.Name, c.ListenOn)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid gRPC port: %w", err)
	}
	_, err = n.namingCli.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: c.Name,
		GroupName:   n.groupOrDefault(n.config.Group),
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register gRPC service: %w", err)
	}
	n.Logger.Info("[Nacos] registered gRPC instance: service=%s group=%s ip=%s port=%d", c.Name, n.groupOrDefault(n.config.Group), ip, port)

	go handleShutdown(n.namingCli, c.Name, ip, port)
	return nil
}

func (n *nacosClient) InitRegisterInstance(c rest.RestConf) error {
	_, ip, portStr := util.GetRegistryParameters(c)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid SERVICE_PORT: %w", err)
	}
	_, err = n.namingCli.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: n.config.DataId,
		GroupName:   n.groupOrDefault(n.config.Group),
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"idc": "shanghai"},
	})
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	n.Logger.Info("[Nacos] registered instance: service=%s group=%s ip=%s port=%d", n.config.DataId, n.groupOrDefault(n.config.Group), ip, port)

	go handleShutdown(n.namingCli, n.config.DataId, ip, port)
	return nil
}

func (n *nacosClient) groupOrDefault(groupName string) string {
	if groupName == "" {
		return "DEFAULT_GROUP"
	}
	return groupName
}

func (n *nacosClient) HasSeverCli(serviceName string) (bool, error) {
	groupName := n.groupOrDefault(n.config.Group)
	n.Logger.Info("[Nacos] querying instances: service=%s group=%s", serviceName, groupName)
	instances, err := n.namingCli.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		HealthyOnly: true,
	})
	if err != nil {
		n.Logger.Info("[Nacos] SelectInstances error: service=%s group=%s err=%v", serviceName, groupName, err)
		return false, err
	}
	if len(instances) > 0 {
		return true, nil
	}
	return false, nil
}

func (n *nacosClient) GetSeverCli(serviceName string) error {
	groupName := n.groupOrDefault(n.config.Group)
	n.Logger.Info("[Nacos] querying instances: service=%s group=%s", serviceName, groupName)
	instances, err := n.namingCli.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		GroupName:   groupName,
		HealthyOnly: true,
	})
	if err != nil {
		n.Logger.Info("[Nacos] SelectInstances error: service=%s group=%s err=%v", serviceName, groupName, err)
		return err
	}
	addrs := make([]string, 0, len(instances))
	for _, inst := range instances {
		n.Logger.Info("[Nacos] found instance: %s:%d healthy=%v enable=%v weight=%f", inst.Ip, inst.Port, inst.Healthy, inst.Enable, inst.Weight)
		addrs = append(addrs, fmt.Sprintf("%s:%d", inst.Ip, inst.Port))
	}
	n.Logger.Info("[Nacos] total instances found: %d", len(addrs))
	n.instances.Store(serviceName+":"+groupName, addrs)
	return nil
}

// autoRefresh 定期刷新
func (n *nacosClient) autoRefresh() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		n.instances.Range(func(key, value any) bool {
			k := key.(string)
			arr := strings.Split(k, ":")
			if len(arr) != 2 {
				return true
			}
			err := n.GetSeverCli(arr[0]) // 忽略错误，保留旧列表
			if err != nil {
				log.Println(err)
			}
			return true
		})
	}
}

// GetHealthyInstances 返回当前健康的实例列表（副本）
func (n *nacosClient) GetHealthyInstances(serviceName string) []string {
	key := serviceName + ":" + n.config.Group
	v, ok := n.instances.Load(key)
	if !ok {
		if err := n.GetSeverCli(serviceName); err != nil {
			log.Println(err)
			return nil
		}
		v, ok = n.instances.Load(key)
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
		log.Println("[ERROR] Deregister failed: %v\n", serviceName, err)
	} else {
		log.Println("[INFO] Deregistered successfully.", serviceName)
	}
	os.Exit(0)
}

func (n *nacosClient) SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf) {
	rpcClientConf.Target = n.config.BuildConfigUrl(serviceName)
}

func (n *nacosClient) GetNacosClient() naming_client.INamingClient {
	return n.namingCli
}
