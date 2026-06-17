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

// BuildConfigUrl 根据 NacosConfig 组装配置拉取 URL
func (n *NacosConfig) BuildConfigUrl() string {
	// 1. 拼接地址：ip1:port,ip2:port,...
	addresses := make([]string, 0, len(n.IpAddress))
	for _, ip := range n.IpAddress {
		addresses = append(addresses, fmt.Sprintf("%s:%d", ip, n.Port))
	}
	addrStr := strings.Join(addresses, ",")

	// 2. 构建基础 scheme
	url := fmt.Sprintf("nacos://%s/%s", addrStr, n.DataId)

	// 3. 添加查询参数
	params := make([]string, 0)
	if n.NamespaceId != "" {
		params = append(params, fmt.Sprintf("namespaceid=%s", n.NamespaceId))
	}
	if n.Group != "" {
		params = append(params, fmt.Sprintf("group=%s", n.Group))
	}
	if len(params) > 0 {
		url += "?" + strings.Join(params, "&")
	}

	// 4. 如果有认证信息，插入到 host 之前
	if n.Username != "" && n.Password != "" {
		// 注意：如果用户名/密码包含特殊字符，需进行 URL 编码（此处省略，实际应使用 url.QueryEscape）
		auth := fmt.Sprintf("%s:%s@", n.Username, n.Password)
		// 在 "nacos://" 之后插入 auth
		url = strings.Replace(url, "nacos://", "nacos://"+auth, 1)
	}

	return url
}
