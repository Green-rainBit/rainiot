// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"
	"rainiot/pkg/cache"

	"github.com/lxzan/gws"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

func IotwsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connId := r.URL.Query().Get("connId")
		upgrader := gws.NewUpgrader(NewHandler(svcCtx, connId), &gws.ServerOption{
			ParallelEnabled:   true,                                 // 开启并行消息处理
			Recovery:          gws.Recovery,                         // 开启异常恢复
			PermessageDeflate: gws.PermessageDeflate{Enabled: true}, // 开启压缩
		})
		socket, err := upgrader.Upgrade(w, r)
		if err != nil {
			return
		}
		if connId != "" {
			svcCtx.Connection.Storage(connId, socket)
		}
		go func() {
			socket.ReadLoop() // 此处阻塞会使请求上下文不能顺利被GC
		}()
	}
}

func NewHandler(svcCtx *svc.ServiceContext, connId string) *Handler {
	return &Handler{
		svcCtx: svcCtx,
		connId: connId,
	}
}

type Handler struct {
	connId string
	svcCtx *svc.ServiceContext
}

func (c *Handler) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	go func() {
		time.Sleep(PingInterval)
		if c.connId == "" { // 缓存设备sn or userid，后续可以通过这个id进行消息推送
			c.ServerOnClose(socket, errors.New("timeout"))
		} else {
			c.svcCtx.Connection.Storage(c.connId, socket)
			c.svcCtx.Redis.SetXX(context.Background(), cache.GetCacheConn(c.connId), c.svcCtx.Config.Name, cache.ConnTime)
		}
	}()
}

func (c *Handler) ServerOnClose(socket *gws.Conn, err error) {
	if err != nil {
		socket.WriteMessage(gws.OpcodeText, []byte(err.Error()))
	}
	if c.connId != "" { // 断开连接时删除缓存
		c.svcCtx.Redis.Del(context.Background(), c.connId)
		c.svcCtx.Connection.Del(c.connId)
	}
	socket.NetConn().Close()
}

func (c *Handler) OnClose(socket *gws.Conn, err error) {
	if c.connId != "" { // 断开连接时删除缓存
		c.svcCtx.Redis.Del(context.Background(), cache.GetCacheConn(c.connId))
		c.svcCtx.Connection.Del(c.connId)
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

	if c.connId == "" {
		if err != nil {
			defer c.ServerOnClose(socket, err)
			return
		}
		req := types.Request{}
		json.Unmarshal(message.Bytes(), &req)
		c.connId = req.Sn
		socket.WriteMessage(message.Opcode, by)
	} else {
		if err != nil {
			defer socket.WriteMessage(message.Opcode, []byte(err.Error()))
			return
		}
		socket.WriteMessage(message.Opcode, by)
	}

}
