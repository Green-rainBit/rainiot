package log

import (
	"fmt"

	logbridge "rainiot/pkg/log/iotlogbridge"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/log/logmulti"

	"github.com/zeromicro/go-zero/core/logx"
)

// Setup 根据配置设置日志系统。
func Setup(logConf logx.LogConf, lokiConf logloki.LokiConf, natsUrls []string) (logx.Writer, error) {
	if lokiConf.Console {
		logConf.Mode = "console" // go-zero 原生控制台输出
	}
	logx.MustSetup(logConf)

	if !lokiConf.Enable {
		return nil, nil
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
	logx.SetWriter(multi)
	return multi, nil
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
