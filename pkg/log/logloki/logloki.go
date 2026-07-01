// Package logloki 实现 go-zero logx.Writer 接口，将日志推送 Grafana Loki。
// 底层使用 gosthell/promtail 库 — 零外部依赖，JSON v1 API + 批量推送。
package logloki

import (
	"fmt"
	"os"
	"time"

	"rainiot/pkg/log/promtail"
	"github.com/zeromicro/go-zero/core/logx"
)

// LokiConf Loki 日志配置。
type LokiConf struct {
	Enable      bool   `json:",optional"`
	Mode        string `json:",optional,default=direct"`
	Url         string `json:",optional"`
	SourceName  string `json:",optional"`
	JobName     string `json:",optional"`
	BatchWait   int    `json:",optional,default=5"`
	BatchSize   int    `json:",optional,default=10000"`
	NatsSubject string `json:",optional,default=rainiot.logs"`
	PrintLevel  string `json:",optional,default=info"`
	SendLevel   string `json:",optional,default=info"`
}

// LogWrite 实现 logx.Writer 接口。
type LogWrite struct {
	client promtail.Client
}

// NewLogWrite 根据配置创建 Loki 日志写入器。
func NewLogWrite(cfg LokiConf) (*LogWrite, error) {
	batchWait := time.Duration(cfg.BatchWait) * time.Second
	if batchWait <= 0 {
		batchWait = 5 * time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	client, err := promtail.NewJSONv1Client(cfg.Url, map[string]string{
		"source": cfg.SourceName,
		"job":    cfg.JobName,
	},
		promtail.WithSendBatchSize(uint(batchSize)),
		promtail.WithSendBatchTimeout(batchWait),
		promtail.WithErrorCallback(func(err error) {
			fmt.Fprintf(os.Stderr, "[logloki] push error: %v\n", err)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("logloki: %w", err)
	}

	client.Infof("loki writer ready: source=%s job=%s", cfg.SourceName, cfg.JobName)
	return &LogWrite{client: client}, nil
}

// ---- logx.Writer 实现 ----

func (l *LogWrite) Alert(v any)                              { l.client.Errorf("alert: %v", v) }
func (l *LogWrite) Close() error                             { l.client.Close(); return nil }
func (l *LogWrite) Severe(v any)                             { l.client.Errorf("severe: %v", v) }
func (l *LogWrite) Stack(v any)                              { l.client.Infof("stack: %v", v) }
func (l *LogWrite) Stat(v any, _ ...logx.LogField)           { l.client.Infof("stat: %v", v) }
func (l *LogWrite) Debug(v any, f ...logx.LogField)          { l.client.Debugf("debug: %v%s", v, ff(f)) }
func (l *LogWrite) Error(v any, f ...logx.LogField)          { l.client.Errorf("error: %v%s", v, ff(f)) }
func (l *LogWrite) Info(v any, f ...logx.LogField)           { l.client.Infof("info: %v%s", v, ff(f)) }
func (l *LogWrite) Slow(v any, f ...logx.LogField)           { l.client.Warnf("slow: %v%s", v, ff(f)) }

func ff(fields []logx.LogField) string {
	if len(fields) == 0 {
		return ""
	}
	s := ""
	for _, f := range fields {
		s += fmt.Sprintf(" %s=%v", f.Key, f.Value)
	}
	return s
}
