// Loki Bridge 服务：从 NATS JetStream 消费日志消息，批量推送到 Loki。
// JetStream 提供持久化保障：bridge 宕机重连后消息不丢。
//
// 架构:
//
//	业务服务 ──(JetStream)──▶ Bridge ──(HTTP)──▶ Loki
//	             持久化         ACK/NAK
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
	"os"
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

// ---- 待确认消息 ----

type pendingMsg struct {
	msg   *nats.Msg
	entry LogEntry
}

// ---- 统计 ----

type stats struct {
	dropped int64 // channel 溢出丢弃
	sent    int64 // 成功推送 Loki 批次
	failed  int64 // 推送 Loki 失败批次
	naked   int64 // NAK 重投计数
}

const (
	defaultStreamName  = "RAINIOT_LOGS"
	defaultStreamTTL   = 24 * time.Hour
	defaultStreamBytes = 5 * 1024 * 1024 * 1024 // 5GB
	streamAckWait       = 30 * time.Second
)

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

	// JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("[Bridge] JetStream context: %v", err)
	}

	// 参数
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

	// 确保 JetStream stream 存在
	streamName := streamNameFromSubject(subject)
	if err := ensureStream(js, streamName, subject); err != nil {
		log.Fatalf("[Bridge] ensure stream: %v", err)
	}

	// 缓冲 channel
	ch := make(chan pendingMsg, batchSize*2)
	st := &stats{}

	// 后台批量推送到 Loki
	go flushLoop(ch, lokiURL, batchSize, time.Duration(flushSec)*time.Second, st)

	// 定期统计
	go func() {
		for range time.Tick(30 * time.Second) {
			log.Printf("[Bridge] stats: sent=%d failed=%d dropped=%d naked=%d",
				st.sent, st.failed, st.dropped, st.naked)
		}
	}()

	// JetStream 队列订阅：同 queue group 轮询，显式 ACK
	sub, err := js.QueueSubscribe(subject, queueGroup, func(msg *nats.Msg) {
		var entry LogEntry
		if err := json.Unmarshal(msg.Data, &entry); err != nil {
			msg.Ack() // 无法解析的消息直接 ACK 丢弃
			return
		}
		select {
		case ch <- pendingMsg{msg: msg, entry: entry}:
		default:
			// channel 满：NAK 延迟重投，不丢消息
			msg.NakWithDelay(1 * time.Second)
			st.dropped++
		}
	},
		nats.AckExplicit(),
		nats.Durable(queueGroup),
		nats.MaxAckPending(batchSize*2),
		nats.AckWait(streamAckWait),
	)
	if err != nil {
		log.Fatalf("[Bridge] JetStream subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("[Bridge] Listening on JetStream subject=%q queue=%q → Loki %s",
		subject, queueGroup, lokiURL)
	log.Printf("[Bridge] Batch: %d entries, Flush: %ds, AckWait: %s",
		batchSize, flushSec, streamAckWait)

	// 阻塞
	select {}
}

// flushLoop 定时批量推送到 Loki，成功后 ACK，失败后 NAK 重投。
func flushLoop(ch <-chan pendingMsg, lokiURL string, maxSize int, interval time.Duration, st *stats) {
	buf := make([]pendingMsg, 0, maxSize)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	doFlush := func() {
		if len(buf) == 0 {
			return
		}

		entries := make([]LogEntry, len(buf))
		for i, p := range buf {
			entries[i] = p.entry
		}

		if err := pushToLoki(lokiURL, entries); err != nil {
			log.Printf("[Bridge] push error: %v, %d entries will be redelivered", err, len(buf))
			st.failed++
			// NAK 所有待处理消息，延迟重投
			for _, p := range buf {
				p.msg.NakWithDelay(5 * time.Second)
			}
			st.naked += int64(len(buf))
			buf = buf[:0]
			return
		}

		// 成功：ACK 所有消息
		for _, p := range buf {
			p.msg.Ack()
		}
		st.sent++
		buf = buf[:0]
	}

	for {
		select {
		case pm, ok := <-ch:
			if !ok {
				doFlush()
				return
			}
			buf = append(buf, pm)
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

// ensureStream 确保 JetStream stream 存在（幂等）。
func ensureStream(js nats.JetStreamContext, name, subject string) error {
	_, err := js.StreamInfo(name)
	if err == nil {
		return nil
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     name,
		Subjects: []string{subject},
		Storage:  nats.FileStorage,
		MaxAge:   defaultStreamTTL,
		MaxBytes: defaultStreamBytes,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[Bridge] JetStream stream created: %s (subject=%s)\n", name, subject)
	return nil
}

// streamNameFromSubject 从 NATS subject 推导 stream 名称。
func streamNameFromSubject(subject string) string {
	s := strings.ReplaceAll(subject, ".", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return strings.ToUpper(s)
}
