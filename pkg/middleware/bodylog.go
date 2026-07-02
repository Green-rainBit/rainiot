// Package middleware 提供 HTTP 中间件。
package middleware

import (
	"bytes"
	"io"
	"net/http"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// RequestBodyLog 记录请求体内容到 logx（共享 go-zero 的 trace/span 上下文）。
// 读取 body 后回填，后续中间件和 handler 仍能正常读取。
func RequestBodyLog(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			next(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			next(w, r)
			return
		}

		r.Body = io.NopCloser(bytes.NewBuffer(body))

		if len(body) > 0 {
			logx.WithContext(r.Context()).Infof("body: %s", string(body))
		}

		next(w, r)
	}
}

func Middleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				logx.Errorf("panic: %v", err)
				httpx.WriteJson(w, http.StatusInternalServerError, "internal server error")
				return
			}
		}()
		next(w, r)
	}
}
