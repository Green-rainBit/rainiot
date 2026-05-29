// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"net/http"
	"time"

	"rainiot/app/iotws/cmd/internal/svc"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

func IotwsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connId := r.URL.Query().Get("connId")
		socket, err := svcCtx.Upgrader.Upgrade(w, r)
		if err != nil {
			return
		}

		if connId != "" {
			socket.Session().Store("connId", connId)
			svcCtx.Connection.Storage(connId, socket)
		}

		go func() {
			socket.ReadLoop() // 此处阻塞会使请求上下文不能顺利被GC
		}()
	}
}
