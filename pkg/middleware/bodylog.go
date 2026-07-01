// Package middleware 提供 HTTP 中间件。
package middleware

import (
	"bytes"
	"io"
	"net/http"

	"github.com/zeromicro/go-zero/core/logx"
)

// RequestBodyLog 记录请求体内容的中间件。
// 将 body 读取后回填，保证后续 handler 仍能读取。
func RequestBodyLog(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			next(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			logx.WithContext(r.Context()).Errorf("read request body: %v", err)
			next(w, r)
			return
		}

		// 回填 body 供后续读取
		r.Body = io.NopCloser(bytes.NewBuffer(body))

		logger := logx.WithContext(r.Context())
		if len(body) > 0 {
			logger.Infof("req: %s", string(body))
		}

		next(w, r)
	}
}
