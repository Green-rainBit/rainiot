package nacos

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/util"
	"strconv"
	"syscall"

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

func (l *nacosClient) GetSeverCli(serviceName, groupName string) (ip string, port uint64, e error) {

	instance, err := l.namingCli.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
		GroupName:   groupName,             // 默认值DEFAULT_GROUP
		Clusters:    []string{"cluster-a"}, // 默认值DEFAULT
	})
	if err != nil {
		return "", 0, nil
	}
	return instance.Ip, instance.Port, nil
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

