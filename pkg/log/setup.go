package log

import (
	"fmt"

	logbridge "rainiot/pkg/log/iotlogbridge"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/log/logmulti"

	"github.com/zeromicro/go-zero/core/logx"
)

// Setup 根据配置设置日志系统。
// logConf: go-zero 文件日志配置（来自 rest.RestConf.Log 或手动 load）
// lokiConf: Loki 日志配置（顶级 Loki 字段）
// natsUrls: NATS 地址，仅 bridge 模式使用
func Setup(logConf logx.LogConf, lokiConf logloki.LokiConf, natsUrls []string) (logx.Writer, error) {
	logx.MustSetup(logConf)

	if !lokiConf.Enable {
		return nil, nil
	}

	switch lokiConf.Mode {
	case LokiModeBridge:
		return setupBridge(lokiConf, natsUrls)
	default:
		return setupDirect(lokiConf)
	}
}

func setupDirect(cfg logloki.LokiConf) (logx.Writer, error) {
	lokiWriter, err := logloki.NewLogWrite(cfg)
	if err != nil {
		return nil, fmt.Errorf("setup loki direct: %w", err)
	}
	multi := logmulti.NewMultiWriter(lokiWriter)
	logx.SetWriter(multi)
	return multi, nil
}

func setupBridge(cfg logloki.LokiConf, natsUrls []string) (logx.Writer, error) {
	subject := cfg.NatsSubject
	if subject == "" {
		subject = "rainiot.logs"
	}
	bridgeWriter, err := logbridge.NewLogWrite(natsUrls, subject, cfg.SourceName, cfg.JobName)
	if err != nil {
		return nil, fmt.Errorf("setup loki bridge: %w", err)
	}
	multi := logmulti.NewMultiWriter(bridgeWriter)
	logx.SetWriter(multi)
	return multi, nil
}
