// Loki Bridge 服务：从 NATS 消费日志消息，批量推送到 Loki。
//
// 架构:
//
//	业务服务 ──(NATS)──▶ Bridge ──(HTTP)──▶ Loki
//
// 用法: logbridge.exe -f etc/logbridge.json
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/core/conf"
)

// ---- 配置 ----

type Config struct {
	Nats NatsConfig `json:"Nats"`
	Loki LokiConfig `json:"Loki"`
}

type NatsConfig struct {
	Urls       []string `json:",optional"`
	Subject    string   `json:",default=rainiot.logs"`
	QueueGroup string   `json:",optional,default=iotlogbridge"`
}

type LokiConfig struct {
	Url       string `json:",default=http://localhost:3100"`
	BatchSize int    `json:",default=5000"`
	FlushSec  int    `json:",default=3"`
}

// ---- 日志条目 ----

type LogEntry struct {
	Timestamp  string `json:"timestamp"`
	Level      string `json:"level"`
	SourceName string `json:"source_name"`
	JobName    string `json:"job"`
	Message    string `json:"message"`
}

// ---- Loki push 格式 ----

type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"` // [["<nano_ts>", "<line>"]]
}

type LokiPush struct {
	Streams []LokiStream `json:"streams"`
}

// ---- 统计 ----

type stats struct {
	dropped int64 // 被丢弃的旧日志计数
	sent    int64 // 成功发送批次计数
	failed  int64 // 发送失败批次计数
}

var configFile = flag.String("f", "etc/logbridge.json", "config file")

func main() {
	flag.Parse()

	var c Config
	conf.MustLoad(*configFile, &c)

	// NATS 连接
	urls := c.Nats.Urls
	if len(urls) == 0 {
		urls = []string{nats.DefaultURL}
	}
	nc, err := nats.Connect(strings.Join(urls, ","),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(3*time.Second),
	)
	if err != nil {
		log.Fatalf("[Bridge] NATS connect: %v", err)
	}
	defer nc.Drain()

	subject := c.Nats.Subject
	if subject == "" {
		subject = "rainiot.logs"
	}
	queueGroup := c.Nats.QueueGroup
	if queueGroup == "" {
		queueGroup = "iotlogbridge"
	}

	lokiURL := c.Loki.Url
	if lokiURL == "" {
		lokiURL = "http://localhost:3100"
	}
	batchSize := c.Loki.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}
	flushSec := c.Loki.FlushSec
	if flushSec <= 0 {
		flushSec = 3
	}

	// 后台缓冲 + 批量推送
	ch := make(chan LogEntry, batchSize*2)
	st := &stats{}
	go flushLoop(ch, lokiURL, batchSize, time.Duration(flushSec)*time.Second, st)

	// 定期打印统计信息
	go func() {
		for range time.Tick(30 * time.Second) {
			log.Printf("[Bridge] stats: sent=%d failed=%d dropped=%d", st.sent, st.failed, st.dropped)
		}
	}()

	// 队列订阅 NATS：同 queue group 的实例轮询分摊，一条消息只被一个实例处理
	nc.QueueSubscribe(subject, queueGroup, func(msg *nats.Msg) {
		var entry LogEntry
		if err := json.Unmarshal(msg.Data, &entry); err != nil {
			return
		}
		// 非阻塞写入 channel，满时不阻塞 NATS 消费
		select {
		case ch <- entry:
		default:
			// channel 满：丢弃本次日志，不阻塞 NATS 消息处理
			st.dropped++
		}
	})

	log.Printf("[Bridge] Listening on NATS subject=%q queue=%q → Loki %s", subject, queueGroup, lokiURL)
	log.Printf("[Bridge] Batch: %d entries, Flush: %ds", batchSize, flushSec)

	// 阻塞
	select {}
}

// flushLoop 定时批量推送到 Loki。
func flushLoop(ch <-chan LogEntry, lokiURL string, maxSize int, interval time.Duration, st *stats) {
	buf := make([]LogEntry, 0, maxSize)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	doFlush := func() {
		if len(buf) == 0 {
			return
		}
		if err := pushToLoki(lokiURL, buf); err != nil {
			log.Printf("[Bridge] push error: %v", err)
			st.failed++
			// 保留 buffer 重试；超出上限时丢弃最旧的一半以防内存溢出
			if len(buf) > maxSize*2 {
				discard := len(buf) / 2
				buf = append(buf[:0], buf[discard:]...)
				st.dropped += int64(discard)
				log.Printf("[Bridge] WARN: buffer overflow, dropped %d oldest entries, remaining %d", discard, len(buf))
			}
			return
		}
		st.sent++
		buf = buf[:0]
	}

	for {
		select {
		case entry, ok := <-ch:
			if !ok {
				doFlush()
				return
			}
			buf = append(buf, entry)
			if len(buf) >= maxSize {
				doFlush()
			}
		case <-ticker.C:
			doFlush()
		}
	}
}

// pushToLoki 将日志条目推送到 Loki HTTP API。
// 按 source_name + job_name + level 分组，保留原始日志时间戳。
func pushToLoki(url string, entries []LogEntry) error {
	type streamKey struct {
		source string
		job    string
		level  string
	}

	streamsMap := make(map[streamKey]*LokiStream)
	var streamKeys []streamKey

	for _, e := range entries {
		key := streamKey{source: e.SourceName, job: e.JobName, level: e.Level}
		s, ok := streamsMap[key]
		if !ok {
			s = &LokiStream{
				Stream: map[string]string{
					"source": e.SourceName,
					"job":    e.JobName,
					"level":  e.Level,
				},
			}
			streamsMap[key] = s
			streamKeys = append(streamKeys, key)
		}

		// 解析原始日志时间戳，失败时退化为当前时间
		ts := parseTimestamp(e.Timestamp)

		line, _ := json.Marshal(map[string]string{
			"msg":        e.Message,
			"@timestamp": e.Timestamp,
		})
		s.Values = append(s.Values, []string{fmt.Sprintf("%d", ts.UnixNano()), string(line)})
	}

	var push LokiPush
	for _, k := range streamKeys {
		push.Streams = append(push.Streams, *streamsMap[k])
	}

	body, _ := json.Marshal(push)
	resp, err := http.Post(url+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("loki returned %d", resp.StatusCode)
	}
	return nil
}

// parseTimestamp 解析 RFC3339Nano 格式的时间戳，失败返回当前时间。
func parseTimestamp(s string) time.Time {
	if s == "" {
		return time.Now()
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Now()
	}
	return t
}
