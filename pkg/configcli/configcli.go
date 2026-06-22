package configcli

type ConfigCli interface {
	// SetGrpcConfig(serviceName string, rpcClientConf *zrpc.RpcClientConf)
	GetHealthyInstances(serviceName string) []string
}
