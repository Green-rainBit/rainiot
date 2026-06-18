package openconfig

import (
	"fmt"
	"strings"
)

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

// BuildConfigUrl 组装 nacos gRPC 服务发现 URL，支持集群模式（多地址逗号分隔）
// 格式: nacos://[user:passwd@]host1:port1,host2:port2/service?namespaceid=xxx&group=xxx
func (n *NacosConfig) BuildConfigUrl(serviceName string) string {
	// 1. 拼接多个 nacos 地址，逗号分隔
	addresses := make([]string, 0, len(n.IpAddress))
	for _, ip := range n.IpAddress {
		addresses = append(addresses, fmt.Sprintf("%s:%d", ip, n.Port))
	}
	addr := strings.Join(addresses, ",")

	// 2. 拼接认证信息（user:passwd@）
	if n.Username != "" && n.Password != "" {
		addr = fmt.Sprintf("%s:%s@%s", n.Username, n.Password, addr)
	}

	// 3. 构建基础 URL: nacos://host1:port1,host2:port2/service
	urlStr := fmt.Sprintf("nacos://%s/%s", addr, serviceName)

	// 4. 添加查询参数
	params := make([]string, 0)
	if n.NamespaceId != "" {
		params = append(params, fmt.Sprintf("namespaceid=%s", n.NamespaceId))
	}
	if n.Group != "" {
		params = append(params, fmt.Sprintf("group=%s", n.Group))
	}
	if len(params) > 0 {
		urlStr += "?" + strings.Join(params, "&")
	}

	return urlStr
}
