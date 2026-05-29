package openconfig

type NacosConfig struct {
	// NacosSeverConfig []constant.ServerConfig
	// NacosAppClientConfig constant.ClientConfig
	IpAddress []string `json:"ipAddress,optional"`
	Port      uint64   `json:"port,optional"`

	Model string `json:"model,optional"`

	NamespaceId string `json:"namespaceId,optional"`
	Username    string `json:"username,optional"`
	Password    string `json:"password,optional"`
	AccessKey   string `json:"accessKey,optional"`
	SecretKey   string `json:"secretKey,optional"`
	RegionId    string `json:"regionId,optional"`

	DataId string `json:"dataId,optional"`
	Group  string `json:"group,optional"`
}
