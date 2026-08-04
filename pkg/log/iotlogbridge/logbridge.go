// Package logbridge 实现 go-zero logx.Writer 接口，将日志发布到 NATS JetStream。
// JetStream 提供持久化保障：NATS 重启日志不丢。
//
// 适用场景：不想在每个服务中直连 Loki，而是通过 NATS 集中转发。
//
//	┌──────────┐  JetStream   ┌───────────────┐   HTTP   ┌──────┐
//	│ 业务服务   │──(pub/ack)──▶│ Loki Bridge    │──(push)─▶│ Loki │
//	└──────────┘  持久化到磁盘  └───────────────┘          └──────┘
package logbridge

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultStreamName  = "RAINIOT_LOGS"
	defaultStreamTTL   = 24 * time.Hour
	defaultStreamBytes = 5 * 1024 * 1024 * 1024 // 5GB
)

// LogEntry 日志条目，通过 NATS 传输。
type LogEntry struct {
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	SourceName string `json:"source_name"`
	JobName    string `json:"job"`
	Message    string `json:"message"`
}

// LogWrite 实现 logx.Writer，将日志发布到 NATS JetStream。
type LogWrite struct {
	conn       *nats.Conn
	js         nats.JetStreamContext
	subject    string
	sourceName string
	jobName    string
	mu         sync.Mutex
	closed     bool
}

// NewLogWrite 创建 JetStream 桥接日志写入器。
// natsUrls: NATS 连接地址列表
// subject: 发布日志的 NATS 主题
// sourceName: 日志来源名称
// jobName: 任务名称
func NewLogWrite(natsUrls []string, subject, sourceName, jobName string) (*LogWrite, error) {
	if len(natsUrls) == 0 {
		natsUrls = []string{nats.DefaultURL}
	}

	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(3 * time.Second),
		nats.Timeout(5 * time.Second),
		nats.RetryOnFailedConnect(true),
	}

	conn, err := nats.Connect(joinUrls(natsUrls), opts...)
	if err != nil {
		return nil, fmt.Errorf("logbridge: connect NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("logbridge: JetStream context: %w", err)
	}

	lw := &LogWrite{
		conn:       conn,
		js:         js,
		subject:    subject,
		sourceName: sourceName,
		jobName:    jobName,
	}

	// 确保 JetStream stream 存在（幂等）
	if err := lw.ensureStream(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("logbridge: ensure stream: %w", err)
	}

	return lw, nil
}

func (l *LogWrite) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return l.conn.Drain()
}

func (l *LogWrite) write(level, msg string) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	entry := LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Level:      level,
		SourceName: l.sourceName,
		JobName:    l.jobName,
		Message:    msg,
	}
	data, _ := json.Marshal(entry)

	// PublishAsync: 非阻塞写 JetStream，NATS 端持久化后返回 ack（不等待）
	if _, err := l.js.PublishAsync(l.subject, data); err != nil {
		fmt.Fprintf(os.Stderr, "[logbridge] JetStream publish error: %v\n", err)
	}
}

// ensureStream 确保 JetStream stream 存在，不存在则创建。
func (l *LogWrite) ensureStream() error {
	streamName := streamNameFromSubject(l.subject)

	_, err := l.js.StreamInfo(streamName)
	if err == nil {
		return nil // stream 已存在
	}

	// 创建 stream
	_, err = l.js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{l.subject},
		Storage:  nats.FileStorage,
		MaxAge:   defaultStreamTTL,
		MaxBytes: defaultStreamBytes,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[logbridge] JetStream stream created: %s (subject=%s)\n", streamName, l.subject)
	return nil
}

// streamNameFromSubject 从 NATS subject 推导 stream 名称。
// "rainiot.logs" → "RAINIOT_LOGS"
func streamNameFromSubject(subject string) string {
	s := strings.ReplaceAll(subject, ".", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return strings.ToUpper(s)
}

// ---- logx.Writer 接口实现 ----

func (l *LogWrite) Alert(v any)       { l.write("alert", fmt.Sprint(v)) }
func (l *LogWrite) Severe(v any)      { l.write("severe", fmt.Sprint(v)) }
func (l *LogWrite) Stack(v any)       { l.write("stack", fmt.Sprint(v)) }
func (l *LogWrite) Stat(v any, _ ...logx.LogField) { l.write("stat", fmt.Sprint(v)) }

func (l *LogWrite) Debug(v any, fields ...logx.LogField) {
	l.write("debug", fmt.Sprintf("%v%s", v, formatFields(fields)))
}

func (l *LogWrite) Error(v any, fields ...logx.LogField) {
	l.write("error", fmt.Sprintf("%v%s", v, formatFields(fields)))
}

func (l *LogWrite) Info(v any, fields ...logx.LogField) {
	l.write("info", fmt.Sprintf("%v%s", v, formatFields(fields)))
}

func (l *LogWrite) Slow(v any, fields ...logx.LogField) {
	l.write("slow", fmt.Sprintf("%v%s", v, formatFields(fields)))
}

func formatFields(fields []logx.LogField) string {
	if len(fields) == 0 {
		return ""
	}
	s := ""
	for _, f := range fields {
		s += fmt.Sprintf(" %s=%v", f.Key, f.Value)
	}
	return s
}

func joinUrls(urls []string) string {
	if len(urls) == 0 {
		return nats.DefaultURL
	}
	s := urls[0]
	for _, u := range urls[1:] {
		s += "," + u
	}
	return s
}
