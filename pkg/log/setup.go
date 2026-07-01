package log

import (
	"fmt"

	logbridge "rainiot/pkg/log/iotlogbridge"
	"rainiot/pkg/log/logloki"
	"rainiot/pkg/log/logmulti"

	"github.com/zeromicro/go-zero/core/logx"
)

// Setup 根据 LogConf 配置日志系统。
// - Loki 未启用时：使用 go-zero 默认文件日志
// - direct 模式：日志同时写文件 + 直推 Loki
// - bridge 模式：日志同时写文件 + 发布到 NATS 主题
// - dual 模式：日志同时写文件 + 直推 Loki（与 direct 相同）
//
// natsUrls 和 natsSubject 仅在 bridge 模式使用。
func Setup(c LogConf, natsUrls []string) (logx.Writer, error) {
	logx.MustSetup(c.LogConf)

	if !c.Loki.Enable {
		return nil, nil
	}

	switch c.Loki.Mode {
	case LokiModeBridge:
		return setupBridge(c.Loki, natsUrls)
	default: // direct / dual 均直推 Loki
		return setupDirect(c.Loki)
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