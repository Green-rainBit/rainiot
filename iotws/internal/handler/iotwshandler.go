// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"context"
	"net/http"
	"time"

	"rainiot/iotws/internal/logic"
	"rainiot/iotws/internal/svc"

	"github.com/lxzan/gws"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

func IotwsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		upgrader := gws.NewUpgrader(NewHandler(svcCtx), &gws.ServerOption{
			ParallelEnabled:   true,                                 // 开启并行消息处理
			Recovery:          gws.Recovery,                         // 开启异常恢复
			PermessageDeflate: gws.PermessageDeflate{Enabled: true}, // 开启压缩
		})
		socket, err := upgrader.Upgrade(w, r)
		if err != nil {
			return
		}
		go func() {
			socket.ReadLoop() // 此处阻塞会使请求上下文不能顺利被GC
		}()
	}
}

func NewHandler(svcCtx *svc.ServiceContext) *Handler {
	return &Handler{
		svcCtx: svcCtx,
	}
}

type Handler struct {
	svcCtx *svc.ServiceContext
}

func (c *Handler) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
}

func (c *Handler) OnClose(socket *gws.Conn, err error) {}

func (c *Handler) OnPing(socket *gws.Conn, payload []byte) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	_ = socket.WritePing(payload)
}

func (c *Handler) OnPong(socket *gws.Conn, payload []byte) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	_ = socket.WritePong(payload)
}

func (c *Handler) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()
	logic.NewIotwsLogic(context.Background(), c.svcCtx).Iotws(message.Bytes())
	socket.WriteMessage(message.Opcode, message.Bytes())
}
