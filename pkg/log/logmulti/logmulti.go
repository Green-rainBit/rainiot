// Package logmulti 实现多路日志写入器。
// 同时写入 go-zero 文件日志和远程日志（Loki / NATS 等），实现双写。
//
// 使用方式:
//
//	lokiWriter, _ := logloki.NewLogWrite(cfg.Loki)
//	multi := logmulti.NewMultiWriter(lokiWriter)
//	logx.SetWriter(multi)
package logmulti

import (
	"github.com/zeromicro/go-zero/core/logx"
)

// MultiWriter 组合多个 logx.Writer，将日志同时写入所有目标。
type MultiWriter struct {
	writers []logx.Writer
}

// NewMultiWriter 创建多路写入器。
// writers 为空时，日志仅由 go-zero 默认文件 Writer 处理。
func NewMultiWriter(writers ...logx.Writer) *MultiWriter {
	return &MultiWriter{writers: writers}
}

func (m *MultiWriter) Alert(v any) {
	for _, w := range m.writers {
		w.Alert(v)
	}
}

func (m *MultiWriter) Close() error {
	for _, w := range m.writers {
		w.Close()
	}
	return nil
}

func (m *MultiWriter) Debug(v any, fields ...logx.LogField) {
	for _, w := range m.writers {
		w.Debug(v, fields...)
	}
}

func (m *MultiWriter) Error(v any, fields ...logx.LogField) {
	for _, w := range m.writers {
		w.Error(v, fields...)
	}
}

func (m *MultiWriter) Info(v any, fields ...logx.LogField) {
	for _, w := range m.writers {
		w.Info(v, fields...)
	}
}

func (m *MultiWriter) Severe(v any) {
	for _, w := range m.writers {
		w.Severe(v)
	}
}

func (m *MultiWriter) Slow(v any, fields ...logx.LogField) {
	for _, w := range m.writers {
		w.Slow(v, fields...)
	}
}

func (m *MultiWriter) Stack(v any) {
	for _, w := range m.writers {
		w.Stack(v)
	}
}

func (m *MultiWriter) Stat(v any, fields ...logx.LogField) {
	for _, w := range m.writers {
		w.Stat(v, fields...)
	}
}