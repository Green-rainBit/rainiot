// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"net/http"
	"runtime/debug"
	"time"

	"rainiot/app/iotws/cmd/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

func IotwsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// connId为提前签发
		connId := r.URL.Query().Get("connId")
		socket, err := svcCtx.Upgrader.Upgrade(w, r)
		if err != nil {
			return
		}

		if connId != "" {
			socket.Session().Store("connId", connId)
			svcCtx.Connection.Store(connId, socket)
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logx.Errorf("[ws] ReadLoop panic connId=%s: %v\n%s", connId, r, debug.Stack())
					_ = socket.NetConn().Close()
					svcCtx.CleanupSocket(socket) // 见下方说明
				}
			}()
			socket.ReadLoop() // 此处阻塞会使请求上下文不能顺利被GC
		}()
	}
}
