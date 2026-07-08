package httpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultDeviceServiceName = "iotdevice_api"
	defaultHTTPTimeout       = 3 * time.Second
	defaultMaxIdleConns      = 100
	defaultMaxConnsPerHost   = 200
)

type LoadBalancer interface {
	Pick(instances []string) string
}

type deviceHttpCli struct {
	fn            func() []string
	retryCount    int
	headers       map[string]string
	balancer      LoadBalancer
	client        *http.Client
	retryInterval time.Duration
}

// RoundRobin 轮询负载均衡
type RoundRobin struct {
	counter int
}

func (lb *RoundRobin) Pick(instances []string) string {
	if len(instances) == 0 {
		return ""
	}
	idx := lb.counter % len(instances)
	lb.counter++
	return instances[idx]
}

func NewDeviceCli(serviceName string, fn func(serviceName string) []string) *deviceHttpCli {
	return &deviceHttpCli{
		fn: func() []string {
			return fn(defaultDeviceServiceName)
		},
		retryCount:    3,
		headers:       map[string]string{"Content-Type": "application/json", "ServiceName": serviceName},
		balancer:      &RoundRobin{},
		client: &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        defaultMaxIdleConns,
				MaxIdleConnsPerHost: defaultMaxIdleConns,
				MaxConnsPerHost:     defaultMaxConnsPerHost,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		retryInterval: 1 * time.Second,
	}
}

func (d *deviceHttpCli) Push(ctx context.Context, connId string, message []byte) ([]byte, bool, error) {

	resp, err := d.do(ctx, http.MethodPost, "/device/connect", connId, bytes.NewBuffer(message), d.headers)
	if err != nil {
		return nil, true, err
	}
	if resp == nil {
		return nil, true, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, true, err
}

// Do 执行 HTTP 请求，自动故障转移
func (d *deviceHttpCli) do(ctx context.Context, method, path, connId string, body io.Reader, headers map[string]string) (*http.Response, error) {
	instances := d.fn()
	if len(instances) == 0 {
		return nil, fmt.Errorf("无可用服务实例")
	}

	// 尝试多个实例（最多 retryCount 次）
	tried := make(map[string]bool)
	for i := 0; i < d.retryCount; i++ {
		// 选择一个实例（负载均衡）
		addr := d.balancer.Pick(instances)
		// 避免重复尝试同一个实例（可能已被标记失败）
		if tried[addr] {
			continue
		}
		tried[addr] = true

		url := fmt.Sprintf("http://%s%s", addr, path)
		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			continue
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("ConnId", connId)

		resp, err := d.client.Do(req)
		if err == nil && resp.StatusCode < 500 {
			// 成功
			return resp, nil
		}
		// 失败：记录错误，稍后重试下一个实例
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		time.Sleep(d.retryInterval)
	}
	return nil, fmt.Errorf("所有实例请求均失败")
}

func (d *deviceHttpCli) Information() string {
	return "deviceHttpCli"
}

func (d *deviceHttpCli) Close() error {
	return nil
}
