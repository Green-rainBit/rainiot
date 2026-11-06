package errorx

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
) // go-zero 已传递依赖，可直接 import

type LogicError struct {
	Code int
	Msg  string
}

func (e LogicError) Error() string {
	return fmt.Sprintf("code:%d, msg:%s", e.Code, e.Msg)
}

func (e LogicError) Is(target error) bool {
	t, ok := target.(LogicError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

func NewMyError(code int, msg string) error {
	return LogicError{Code: code, Msg: msg}
}

type MyErrorWithTrace struct {
	err   error
	Trace string
}

func NewMyErrorWithCtx(ctx context.Context, e error) error {
	if e == nil {
		return nil
	}
	return MyErrorWithTrace{err: e, Trace: TraceIDFromContext(ctx)}
}

func (e MyErrorWithTrace) Error() string {
	if e.err == nil {
		return fmt.Sprintf("{trace:%s}", e.Trace)
	}
	return fmt.Sprintf("{trace:%s, msg:%s}", e.Trace, e.err.Error())
}

func (e MyErrorWithTrace) Unwrap() error { return e.err }

// TraceIDFromContext 从 context 提取 traceID（go-zero 内部同款实现）。
func TraceIDFromContext(ctx context.Context) string {
	sc := oteltrace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// ToStatusError 把业务错误转成 gRPC 状态错误。
// gRPC 会把普通 error 统一包装成 codes.Unknown，原始 LogicError 类型在
// 客户端已经丢失，因此服务端必须在边界处显式转换。
func ToStatusError(err error) error {
	if err == nil {
		return nil
	}
	var le LogicError
	if errors.As(err, &le) {
		return status.Error(codes.FailedPrecondition, le.Error())
	}
	return err
}

// UnaryServerInterceptor 返回把 LogicError 转成 gRPC 状态错误的拦截器。
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		return resp, ToStatusError(err)
	}
}

// IsLogicError 判断错误是否为业务错误。
// 支持进程内的 LogicError，以及跨 gRPC 后保留的 FailedPrecondition/InvalidArgument 状态。
func IsLogicError(err error) bool {
	var le LogicError
	if errors.As(err, &le) {
		return true
	}
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

// ShouldFallback 判断错误是否值得换下一个客户端/实例重试。
// 业务错误和调用方主动取消都不应降级。
func ShouldFallback(err error) bool {
	if err == nil || IsLogicError(err) {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

// HTTPErrorHandler 把业务错误编码成 JSON 响应，REST 服务启动时注册：
//
//	httpx.SetErrorHandler(errorx.HTTPErrorHandler)
func HTTPErrorHandler(err error) (int, any) {
	var le LogicError
	if errors.As(err, &le) {
		return http.StatusBadRequest, map[string]any{
			"code": le.Code,
			"msg":  le.Msg,
		}
	}
	return http.StatusInternalServerError, map[string]any{
		"code": http.StatusInternalServerError,
		"msg":  err.Error(),
	}
}
