package httpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"net/http"
	"sync/atomic"
)

const (
	defaultHTTPTimeout     = 3 * time.Second
	defaultMaxIdleConns    = 100
	defaultMaxConnsPerHost = 200
)

type LoadBalancer interface {
	Pick(instances []string) string
}

type WsHttpCliInterface interface {
	Push(ctx context.Context, addr, event, connId string, message []byte) ([]byte, error)
}

type wsHttpCli struct {
	gethttpcli    func(string) string
	retryCount    int
	balancer      LoadBalancer
	client        *http.Client
	retryInterval time.Duration
}

// RoundRobin 轮询负载均衡
type RoundRobin struct {
	counter atomic.Uint64
}

func (lb *RoundRobin) Pick(instances []string) string {
	if len(instances) == 0 {
		return ""
	}
	n := lb.counter.Add(1) - 1 // 第一次取 0
	return instances[n%uint64(len(instances))]
}

func NewDeviceCli() *wsHttpCli {
	return &wsHttpCli{
		retryCount: 3,
		balancer:   &RoundRobin{},
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

func (d *wsHttpCli) Push(ctx context.Context, addr, event, connId string, message []byte) ([]byte, error) {
	headers := map[string]string{
		"Content-Type": "application/json",
		"ConnId":       connId,
		"Event":        event,
	}
	resp, err := d.do(ctx, http.MethodPost, "/notice", connId, bytes.NewBuffer(message), headers)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return body, err
}

// Do 执行 HTTP 请求，自动故障转移
func (d *wsHttpCli) do(ctx context.Context, method, path, connId string, body io.Reader, headers map[string]string) (*http.Response, error) {
	// addr := d.gethttpcli(seviceName)
	// if addr == "" {
	// 	return nil, fmt.Errorf("无可用服务实例")
	// }
	// todo 重构想要动态注册，不能写死配置
	addr := ""
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("读取 body 失败: %w", err)
		}
	}
	// 尝试多次（最多 retryCount 次）
	for i := 0; i < d.retryCount; i++ {
		url := fmt.Sprintf("http://%s%s", addr, path)

		buf := bytes.NewBuffer(bodyBytes)
		req, err := http.NewRequestWithContext(ctx, method, url, buf)
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
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.retryInterval):
		}
	}
	return nil, fmt.Errorf("所有实例请求均失败")
}

func (d *wsHttpCli) Information() string {
	return "deviceHttpCli"
}

func (d *wsHttpCli) Close() error {
	return nil
}
