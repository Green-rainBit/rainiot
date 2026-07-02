package errors

import (
	"context"
	"fmt"

	oteltrace "go.opentelemetry.io/otel/trace"
) // go-zero 已传递依赖，可直接 import

type LogicError struct {
	Code int
	Msg  string
}

func (e LogicError) Error() string {
	return fmt.Sprintf("code:%d, msg:%s", e.Code, e.Msg)
}
func NewMyError(code int, msg string) error {
	return LogicError{Code: code, Msg: msg}
}

type MyErrorWithTrace struct {
	err   error
	Trace string
}

func NewMyErrorWithTrace(trace string, e error) error {
	return MyErrorWithTrace{Trace: trace, err: e}
}

func (e MyErrorWithTrace) Error() string {
	return fmt.Sprintf("{trace:%s, msg:%s}", e.Trace, e.err.Error())
}

// TraceIDFromContext 从 context 提取 traceID（go-zero 内部同款实现）。
func TraceIDFromContext(ctx context.Context) string {
	sc := oteltrace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// 业务逻辑里直接用：传 context，错误里自动带 traceID
func NewMyErrorWithCtx(ctx context.Context, e error) error {
	return MyErrorWithTrace{
		err:   e,
		Trace: TraceIDFromContext(ctx),
	}
}
