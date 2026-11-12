package openconfig

import (
	"fmt"

	"strings"
	"sync"

	"rainiot/pkg/client/devicecli/grpc/instances"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/resolver"
)

type DeviCliConfig struct {
	RpcCliConfig struct {
		zrpc.RpcClientConf
		Model             string `json:""`
		DeviceServiceName string `json:",default=iotdevice.grpc"`
	} `json:""`
	DeviceServers struct {
		Httpc []string `json:""`
		Grpc  []string `json:""`
	}
	MqConfig *MQConfig `json:""`

	mu sync.RWMutex
}

func (c *DeviCliConfig) GetHealthyInstances(model string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	switch model {
	case "httpc":
		return c.DeviceServers.Httpc
	case "grpc":
		return c.DeviceServers.Grpc
	}
	return nil
}

// Lock 在热更新写入前加锁，返回解锁函数
func (c *DeviCliConfig) Lock() func() {
	c.mu.Lock()
	return c.mu.Unlock
}

func (c *DeviCliConfig) BuildDeviceServiceUrl(ocp OpenConfig) {
	if c.RpcCliConfig.Model == "instances" {
		resolver.Register(instances.NewBuilder(func() []string {
			return c.GetHealthyInstances("grpc")
		}))
		c.RpcCliConfig.RpcClientConf.Target = "instances://" + c.RpcCliConfig.DeviceServiceName
	}
	if c.RpcCliConfig.Model == "nacos" {
		c.RpcCliConfig.RpcClientConf.Target = ocp.RegistryConfig.BuildConfigUrl(c.RpcCliConfig.DeviceServiceName)
	}
}

type OpenConfig struct {
	ConfigModel   string `json:"configModel"`
	RegistryModel string `json:"registryModel"`

	ConfigConfig   NacosConfig `json:"configConfig"`
	RegistryConfig NacosConfig `json:"registryConfig"`
}

// MQConfig 消息队列配置
type MQConfig struct {
	Type     string         `json:"type"`  // "rabbitmq" 或 "nats"
	Topic    string         `json:"topic"` // 发布/订阅主题 rabbitmq对应队列
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	NATS     NATSConfig     `json:"nats"`
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
	Addresses        []string `json:"addresses"`        // NATS 服务地址列表，多个地址构成集群
	QueueGroupPrefix string   `json:"queueGroupPrefix"` // 工作队列组名，非空即为工作队列模式
	DurablePrefix    string   `json:"durablePrefix"`    // JetStream 持久消费者前缀，未设置回退到 Subject
	AutoProvision    bool     `json:"autoProvision"`    // 自动创建 JetStream Stream
	SubscribersCount int      `json:"subscribersCount"` // 并发消费者数量，0 表示单协程
	MaxAckPending    int      `json:"maxAckPending"`    // 最大未确认消息数，JetStream push 窗口大小
}

type NacosConfig struct {
	IpAddress []string `json:"ipAddress"`
	Port      uint64   `json:"port"`

	NamespaceId string `json:"namespaceId"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	AccessKey   string `json:"accessKey"`
	SecretKey   string `json:"secretKey"`
	RegionId    string `json:"regionId"`

	DataId string `json:"dataId"`
	Group  string `json:"group"`
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
