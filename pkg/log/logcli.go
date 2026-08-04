package log

import (
	"github.com/zeromicro/go-zero/core/logx"
)

type Logger interface {
	Alert(v any)
	Close() error
	Debug(v any, fields ...logx.LogField)
	Error(v any, fields ...logx.LogField)
	Info(v any, fields ...logx.LogField)
	Severe(v any)
	Slow(v any, fields ...logx.LogField)
	Stack(v any)
	Stat(v any, fields ...logx.LogField)
}
