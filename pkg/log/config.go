// Package log 提供统一的日志配置和 Writer 工厂。
// 支持三种 Loki 写入模式：直写(direct)、桥接(bridge)、双写(dual)。
package log

import (
	"rainiot/pkg/log/logloki"

	"github.com/zeromicro/go-zero/core/logx"
)

// LokiMode 定义 Loki 日志写入模式。
type LokiMode = string

const (
	LokiModeDirect = "direct" // 直写：HTTP 直接推送到 Loki
	LokiModeBridge = "bridge" // 桥接：发布到 NATS，由 Bridge 服务消费后推 Loki
	LokiModeDual   = "dual"   // 双写：同时写文件 + 直推 Loki
)

// LogConf 统一日志配置。
type LogConf struct {
	logx.LogConf
	// Loki Loki 日志配置，可选。
	Loki logloki.LokiConf `json:",optional"`
}