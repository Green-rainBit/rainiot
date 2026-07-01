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
	Urls    []string `json:",optional"`
	Subject string   `json:",default=rainiot.logs"`
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
	go flushLoop(ch, lokiURL, batchSize, time.Duration(flushSec)*time.Second)

	// 订阅 NATS
	nc.Subscribe(subject, func(msg *nats.Msg) {
		var entry LogEntry
		if err := json.Unmarshal(msg.Data, &entry); err != nil {
			return
		}
		ch <- entry
	})

	log.Printf("[Bridge] Listening on NATS subject=%q → Loki %s", subject, lokiURL)
	log.Printf("[Bridge] Batch: %d entries, Flush: %ds", batchSize, flushSec)

	// 阻塞
	select {}
}

// flushLoop 定时批量推送到 Loki。
func flushLoop(ch <-chan LogEntry, lokiURL string, maxSize int, interval time.Duration) {
	buf := make([]LogEntry, 0, maxSize)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	doFlush := func() {
		if len(buf) == 0 {
			return
		}
		if err := pushToLoki(lokiURL, buf); err != nil {
			log.Printf("[Bridge] push error: %v", err)
			// 失败不丢数据，下次重试（简化实现：内存兜底）
			if len(buf) < maxSize*4 {
				return // 保留在 buffer 中等待下次 flush
			}
		}
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
func pushToLoki(url string, entries []LogEntry) error {
	// 按 source_name + job_name 分组
	streamsMap := make(map[string]*LokiStream)
	var streamKeys []string

	for _, e := range entries {
		key := e.SourceName + "\x00" + e.JobName
		s, ok := streamsMap[key]
		if !ok {
			s = &LokiStream{
				Stream: map[string]string{"source": e.SourceName, "job": e.JobName},
			}
			streamsMap[key] = s
			streamKeys = append(streamKeys, key)
		}
		line, _ := json.Marshal(map[string]string{
			"level":      e.Level,
			"source":     e.SourceName,
			"msg":        e.Message,
			"@timestamp": e.Timestamp,
		})
		ts := time.Now().UnixNano()
		s.Values = append(s.Values, []string{fmt.Sprintf("%d", ts), string(line)})
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


