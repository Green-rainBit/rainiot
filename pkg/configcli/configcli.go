package configcli

import (
	"rainiot/pkg/configcli/nacos"
	"rainiot/pkg/openconfig"
)

type ConfigCli interface {
	GetHealthyInstances(serviceName string) []string
}

type ConfigClient interface {
	SyncConfig(onChange func(data string)) (string, error)
}

func SyncConfig(config openconfig.OpenConfig, sync func(data string)) (string, bool, error) {
	var cfClient ConfigClient
	var err error
	switch config.ConfigModel {
	case "nacos":
		cfClient, err = nacos.NewNacosClient(config.ConfigConfig)
	default:
		cfClient = nil
	}
	if err != nil {
		return "", false, err
	}
	if cfClient == nil {
		return "", false, nil
	}
	cf, err := cfClient.SyncConfig(sync)
	return cf, true, err
}
