package alarm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel/trace"
)

const (
	webhookBaseURL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send"
	envKey         = "WECHAT_WEBHOOK_KEY"
)

// Sender 告警发送器接口。
type Sender interface {
	// Send 发送告警消息到企业微信。自动从 ctx 中提取 trace/span 信息附加到消息头部，
	// 与 logx.WithContext(ctx) 的行为一致。
	Send(ctx context.Context, message string) error
}

// Config 企业微信 Webhook 配置。
type Config struct {
	Key     string `json:"key,optional"`     // webhook key，为空时从环境变量 WECHAT_WEBHOOK_KEY 读取
	Enabled bool   `json:"enabled,optional"` // 是否启用告警
	Timeout int    `json:"timeout,optional"` // HTTP 超时（秒），默认 10
}

// wechatSender 企业微信群机器人实现。
type wechatSender struct {
	key    string
	client *http.Client
}

// textMsg 企业微信 text 消息体。
type textMsg struct {
	Content string `json:"content"`
}

// webhookReq 企业微信 webhook 请求体。
type webhookReq struct {
	MsgType string   `json:"msgtype"`
	Text    *textMsg `json:"text,omitempty"`
}

// webhookResp 企业微信 webhook 响应体。
type webhookResp struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// New 创建企业微信告警发送器。返回 nil 表示未启用。
func New(cfg Config) Sender {
	key := cfg.Key
	if key == "" {
		key = os.Getenv(envKey)
	}
	if key == "" || !cfg.Enabled {
		logx.Info("alarm: webhook key not configured, alarm disabled")
		return nil
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10
	}

	logx.Info("alarm: WeChat Work webhook sender initialized")
	return &wechatSender{
		key: key,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        5,
				MaxIdleConnsPerHost: 5,
			},
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

// Send 发送文本告警消息。会从 ctx 中提取 trace/span，格式化后拼入消息。
func (s *wechatSender) Send(ctx context.Context, message string) error {
	content := formatMessage(ctx, message)
	req := webhookReq{
		MsgType: "text",
		Text: &textMsg{
			Content: content,
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	url := fmt.Sprintf("%s?key=%s", webhookBaseURL, s.key)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	var wr webhookResp
	if err := json.NewDecoder(resp.Body).Decode(&wr); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if wr.ErrCode != 0 {
		return fmt.Errorf("wechat api: errcode=%d errmsg=%s", wr.ErrCode, wr.ErrMsg)
	}
	return nil
}

// formatMessage 从 ctx 提取 trace/span 并拼装告警消息。
// 效果类似 logx.WithContext(ctx) 会在日志中带出 trace/span 字段。
func formatMessage(ctx context.Context, message string) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		return message
	}

	var sb strings.Builder
	sb.WriteString("[")
	if tid := spanCtx.TraceID().String(); tid != "" {
		sb.WriteString("trace:")
		sb.WriteString(tid)
	}
	if sid := spanCtx.SpanID().String(); sid != "" {
		if sb.Len() > 1 {
			sb.WriteString(" ")
		}
		sb.WriteString("span:")
		sb.WriteString(sid)
	}
	sb.WriteString("] ")
	sb.WriteString(message)
	return sb.String()
}
