package openconfig

import (
	"fmt"
	"strings"
)

type OpenConfig struct {
	ConfigModel   string `json:"configModel,optional"`
	RegistryModel string `json:"registryModel,optional"`

	ConfigConfig   NacosConfig `json:"configConfig,optional"`
	RegistryConfig NacosConfig `json:"registryConfig,optional"`
}

// MQConfig 消息队列配置
type MQConfig struct {
	Type     string         `json:"type"` // "rabbitmq" 或 "nats"
	RabbitMQ RabbitMQConfig `json:"rabbitmq,optional"`
	NATS     NATSConfig     `json:"nats,optional"`
}

type RabbitMQConfig struct {
	Addresses       []string `json:"addresses"`
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	Exchange        string   `json:"exchange"`
	ExchangeType    string   `json:"exchangeType"`
	DeclareExchange bool     `json:"declareExchange"`
}

type NATSConfig struct {
	Addresses        []string `json:"addresses"`                  // NATS 服务地址列表，多个地址构成集群
	Subject          string   `json:"subject"`                    // 发布/订阅主题
	QueueGroupPrefix string   `json:"queueGroupPrefix,optional"`  // 工作队列组名，非空即为工作队列模式
	DurablePrefix    string   `json:"durablePrefix,optional"`     // JetStream 持久消费者前缀，未设置回退到 Subject
	AutoProvision    bool     `json:"autoProvision,optional"`     // 自动创建 JetStream Stream
	SubscribersCount int      `json:"subscribersCount,optional"`  // 并发消费者数量，0 表示单协程
}

type NacosConfig struct {
	IpAddress []string `json:"ipAddress,optional"`
	Port      uint64   `json:"port,optional"`

	NamespaceId string `json:"namespaceId,optional"`
	Username    string `json:"username,optional"`
	Password    string `json:"password,optional"`
	AccessKey   string `json:"accessKey,optional"`
	SecretKey   string `json:"secretKey,optional"`
	RegionId    string `json:"regionId,optional"`

	DataId string `json:"dataId,optional"`
	Group  string `json:"group,optional"`
}

// BuildConfigUrl 组装 go-zero gRPC nacos 服务发现 URL
// 格式: nacos://[user:passwd@]host:port/service?namespaceid=xxx&group=xxx
func (n *NacosConfig) BuildConfigUrl(serviceName string) string {
	// 1. 取首个 nacos 地址
	addr := ""
	if len(n.IpAddress) > 0 {
		addr = fmt.Sprintf("%s:%d", n.IpAddress[0], n.Port)
	}

	// 2. 拼接认证信息（user:passwd@）
	if n.Username != "" && n.Password != "" {
		addr = fmt.Sprintf("%s:%s@%s", n.Username, n.Password, addr)
	}

	// 3. 构建基础 URL: nacos://host:port/service
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
