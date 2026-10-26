package log

import (
	"fmt"

	logbridge "rainiot/pkg/log/iotlogbridge"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/log/logmulti"

	"github.com/zeromicro/go-zero/core/logx"
)

type Loggerx interface {
	Info(msg string, keysAndValues ...interface{})
	Error(err error, msg string, keysAndValues ...interface{})
	Close() error
}

type loxLogger struct {
	logx.Writer
}

func (c loxLogger) Info(msg string, kv ...any) {
	logx.Infof("%s %v", msg, kv)
}

func (c loxLogger) Error(err error, msg string, kv ...any) {
	logx.Errorf("%s: %v %v", msg, err, kv)
}

type defaultCronLogger struct{}

func (defaultCronLogger) Info(msg string, kv ...any) {
	logx.Infof("%s %v", msg, kv)
}
func (defaultCronLogger) Error(err error, msg string, kv ...any) {
	logx.Errorf("%s: %v %v", msg, err, kv)
}
func (defaultCronLogger) Close() error { return nil }

// Setup 根据配置设置日志系统。
func Setup(logConf logx.LogConf, lokiConf logloki.LokiConf, natsUrls []string) (Loggerx, error) {
	if lokiConf.Console {
		logConf.Mode = "console" // go-zero 原生控制台输出
	}
	if err := logx.SetUp(logConf); err != nil { // 不要 MustSetup，让错误可返回
		return nil, err
	}

	if !lokiConf.Enable {
		return defaultCronLogger{}, nil
	}

	var remoteWriter logx.Writer
	var err error
	switch lokiConf.Mode {
	case LokiModeBridge:
		remoteWriter, err = newBridgeWriter(lokiConf, natsUrls)
	default:
		remoteWriter, err = logloki.NewLogWrite(lokiConf)
	}
	if err != nil {
		return nil, err
	}

	multi := logmulti.NewMultiWriter(remoteWriter)
	logx.AddWriter(multi) // 保留本地 file/console writer

	return loxLogger{}, nil
}

func newBridgeWriter(cfg logloki.LokiConf, natsUrls []string) (logx.Writer, error) {
	subject := cfg.NatsSubject
	if subject == "" {
		subject = "rainiot.logs"
	}
	w, err := logbridge.NewLogWrite(natsUrls, subject, cfg.SourceName, cfg.JobName)
	if err != nil {
		return nil, fmt.Errorf("setup loki bridge: %w", err)
	}
	return w, nil
}
