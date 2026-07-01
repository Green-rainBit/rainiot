package logloki

import (
	"log"
	"os"
	"time"

	"github.com/afiskon/promtail-client/promtail"
	"github.com/zeromicro/go-zero/core/logx"
)

var loki promtail.Client

type Lokiconfig struct {
	Url        string            `json:",optional"`
	SourceName string            `json:",optional"`
	JobName    string            `json:",optional"`
	SendLevel  promtail.LogLevel `json:",optional"`
	PrintLevel promtail.LogLevel `json:",optional"`
}

func register(url, source_name, job_name string, sendLevel, PrintLevel promtail.LogLevel) promtail.Client {

	labels := "{source=\"" + source_name + "\",job=\"" + job_name + "\"}"
	conf := promtail.ClientConfig{
		PushURL:            url + "/api/prom/push",
		Labels:             labels,
		BatchWait:          5 * time.Second,
		BatchEntriesNumber: 10000,
		SendLevel:          sendLevel,
		PrintLevel:         PrintLevel,
	}

	loki, err := promtail.NewClientProto(conf)
	if err != nil {
		log.Printf("promtail.NewClient: %s\n", err)
		os.Exit(1)
	}
	loki.Infof("loki up sucessfully!")
	return loki
}

type LogWrite struct {
	// 注册loki
	loki promtail.Client
}

// NewLogWrite 创建日志写入器
// lokiUrl 地址
// sourceName 源名称
// jobName 任务名称
// sendLevel 发送级别
// PrintLevel 打印级别
func NewLogWrite(lokiUrl, sourceName, jobName string, sendLevel, PrintLevel promtail.LogLevel) *LogWrite {
	return &LogWrite{
		loki: register(lokiUrl, sourceName, jobName, sendLevel, PrintLevel),
	}
}

func (l *LogWrite) Alert(v any) {
	l.loki.Infof("err: %v", v)
}

func (l *LogWrite) Close() error {
	l.loki.Shutdown()
	return nil
}

func (l *LogWrite) Debug(v any, fields ...logx.LogField) {
	if len(fields) > 0 {
		l.loki.Debugf("err: %v, file:%s", v, fields[0].Value)
	} else {
		l.loki.Debugf("err: %v", v)
	}
}

func (l *LogWrite) Error(v any, fields ...logx.LogField) {

	if len(fields) > 0 {
		l.loki.Errorf("err: %v, file:%s", v, fields[0].Value)
	} else {
		l.loki.Errorf("err: %v", v)
	}
}

func (l *LogWrite) Info(v any, fields ...logx.LogField) {
	if len(fields) > 0 {
		l.loki.Infof("info: %v, file:%s", v, fields[0].Value)
	} else {
		l.loki.Infof("info: %v", v)
	}
}

func (l *LogWrite) Severe(v any) {
	l.loki.Errorf("err: %v", v)
}

func (l LogWrite) Slow(v any, fields ...logx.LogField) {
	if len(fields) > 0 {
		l.loki.Warnf("err: %v, file:%s", v, fields[0].Value)
	} else {
		l.loki.Warnf("err: %v", v)
	}
}

func (l *LogWrite) Stack(v any) {
	l.loki.Infof("err: %v", v)
}

func (l *LogWrite) Stat(v any, fields ...logx.LogField) {
	l.loki.Infof("err: %v", v)
}
