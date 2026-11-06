package nacos

import (
	"rainiot/pkg/openconfig"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type nacosClient struct {
	config  openconfig.NacosConfig
	confCli config_client.IConfigClient
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

	configClient, err := clients.NewConfigClient(
		nacosClientParam,
	)
	if err != nil {
		return nil, err
	}
	nc := &nacosClient{
		config:  config,
		confCli: configClient,
	}

	return nc, nil

}

func (n *nacosClient) SyncConfig(sync func(data string)) (string, error) {
	configstring, err := n.confCli.GetConfig(vo.ConfigParam{
		DataId: n.config.DataId,
		Group:  n.config.Group,
	})
	if err != nil {
		return "", err
	}
	onChange := func(namespace, group, dataId, data string) {
		sync(data)
	}
	err = n.confCli.ListenConfig(vo.ConfigParam{
		DataId:   n.config.DataId,
		Group:    n.config.Group,
		OnChange: onChange,
	})
	if err != nil {
		return "", err
	}
	return configstring, nil
}

func (n *nacosClient) groupOrDefault(groupName string) string {
	if groupName == "" {
		return "DEFAULT_GROUP"
	}
	return groupName
}
