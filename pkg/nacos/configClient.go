package nacos

import (
	"rainiot/pkg/openconfig"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

func InitNacosConfig(config openconfig.NacosConfig, onChange func(namespace, group, dataId, data string)) error {
	if config.Model == "local" || len(config.IpAddress) == 0 {
		return nil
	}
	nacosConfigs := []constant.ServerConfig{}
	for _, ipAddress := range config.IpAddress {
		nacosConfigs = append(nacosConfigs, *constant.NewServerConfig(ipAddress, config.Port))
	}
	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig: constant.NewClientConfig(
				constant.WithNamespaceId(config.NamespaceId), //当namespace是public时，此处填空字符串。
				constant.WithTimeoutMs(5000),
				constant.WithNotLoadCacheAtStart(true),
				constant.WithLogDir("/tmp/nacos/log"),
				constant.WithCacheDir("/tmp/nacos/cache"),
				constant.WithLogLevel("debug"),
				constant.WithAccessKey(config.AccessKey),
				constant.WithSecretKey(config.SecretKey),
				constant.WithRegionId(config.RegionId),
			),
			ServerConfigs: nacosConfigs,
		},
	)
	if err != nil {
		return err
	}
	err = configClient.ListenConfig(vo.ConfigParam{
		DataId:   config.DataId,
		Group:    config.Group,
		OnChange: onChange,
	})
	if err != nil {
		return err
	}
	return nil
}
