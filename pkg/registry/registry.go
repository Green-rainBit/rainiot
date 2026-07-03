package registry

import (
	"rainiot/pkg/devicecli/grpc/rpcn"
	"rainiot/pkg/openconfig"
	"rainiot/pkg/registry/nacos"

	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/resolver"
)

type Registry interface {
	InitRegisterInstanceGrpc(c zrpc.RpcServerConf) error
	InitRegisterInstance(c rest.RestConf) error
	// HasSeverCli(serviceName string) (bool, error)
	// GetSeverCli(serviceName string) error
	// GetHealthyInstances(serviceName string) []string
	// SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf)

}

func NewRegistry(config openconfig.OpenConfig) (Registry, bool, error) {
	var registry Registry
	switch config.RegistryModel {
	case "nacos":
		nacoscli, err := nacos.NewNacosClient(config.RegistryConfig)
		if err != nil {
			return nil, false, err
		}
		resolver.Register(rpcn.NewBuilder(nacoscli.GetNacosClient()))
		registry = nacoscli
	default:
		return nil, false, nil
	}
	return registry, true, nil
}
