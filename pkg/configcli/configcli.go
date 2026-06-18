package configcli

import "github.com/zeromicro/go-zero/zrpc"

type ConfigCli interface {
	SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf)
	GetHealthyInstances(serviceName string) []string
}
