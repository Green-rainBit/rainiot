// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"

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
	sn     string
	svcCtx *svc.ServiceContext
}

func (c *Handler) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	go func() {
		time.Sleep(2 * time.Second)
		if c.sn == "" { // 缓存设备sn
			c.ServerOnClose(socket, errors.New("timeout"))
		}
	}()
}

func (c *Handler) ServerOnClose(socket *gws.Conn, err error) {
	if err != nil {
		socket.WriteMessage(gws.OpcodeText, []byte(err.Error()))
	}
	if c.sn != "" { // 断开连接时删除缓存
		c.svcCtx.Redis.Del(context.Background(), c.sn)
		c.svcCtx.Connection.Del(c.sn)
	}
	socket.NetConn().Close()
}

func (c *Handler) OnClose(socket *gws.Conn, err error) {
	if c.sn != "" { // 断开连接时删除缓存
		c.svcCtx.Redis.Del(context.Background(), c.sn)
		c.svcCtx.Connection.Del(c.sn)
	}
}

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
	by, err := logic.NewIotwsLogic(context.Background(), c.svcCtx).Iotws(message.Bytes())

	if c.sn != "" {
		if err != nil {
			defer c.ServerOnClose(socket, err)
			return
		}
		socket.WriteMessage(message.Opcode, by)
	} else {
		c.sn = string(message.Bytes())
		c.svcCtx.Connection.Storage(c.sn, socket)
		socket.WriteMessage(message.Opcode, by)
	}

}
