package devicecli

import "errors"

var (
	// ErrNoClientAvailable 未配置任何可用客户端时返回。
	ErrNoClientAvailable = errors.New("no client available, check model configuration")
)
