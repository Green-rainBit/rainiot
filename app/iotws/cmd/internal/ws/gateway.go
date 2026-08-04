package ws

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"rainiot/pkg/cache"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

var serverError = []byte("server error")

func NewGatewayr(serverName string, conn Connection, redis *redis.ClusterClient) *Gateway {
	return &Gateway{
		Logger:     logx.WithContext(context.Background()),
		connection: conn,
		redis:      redis,
		serverName: serverName,
	}
}

type Gateway struct {
	logx.Logger
	serverName string
	Fn         func(connId string, message []byte) ([]byte, bool, error)
	connection Connection
	redis      *redis.ClusterClient
}

func (c *Gateway) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	SetKeepAlive(socket.NetConn())
}

func (c *Gateway) ServerOnClose(socket *gws.Conn, err error) {
	if err != nil {
		socket.WriteMessage(gws.OpcodeText, []byte(err.Error()))
	}
	connId, ok := socket.Session().Load("connId")
	if ok {
		switch coid := connId.(type) {
		case string:
			c.redis.Del(context.Background(), cache.GetCacheConn(coid))
			c.connection.Del(coid)
		default:

		}
	}
	socket.NetConn().Close()
}

// CloseAll 优雅关闭所有 WebSocket 连接。
// 进程收到关闭信号时由 shutdown listener 调用:对每个连接先发送关闭帧
// (1001 = Going Away),让设备即时感知并重连到其他节点,再关闭底层连接。
// WriteClose 内部为 CAS 幂等,重复调用安全;关闭会触发 OnClose 回调,
// 由其负责清理 redis 中的 connId 缓存与连接表。
func (c *Gateway) CloseAll() {
	c.connection.Range(func(key, value any) bool {
		if socket, ok := value.(*gws.Conn); ok {
			_ = socket.WriteClose(1001, []byte("server shutting down"))
			_ = socket.NetConn().Close()
		}
		return true
	})
}

func (c *Gateway) OnClose(socket *gws.Conn, err error) {
	connId, ok := socket.Session().Load("connId")
	if ok {
		switch coid := connId.(type) {
		case string:
			c.redis.Del(context.Background(), cache.GetCacheConn(coid))
			c.connection.Del(coid)
		default:

		}
	}
}

func (c *Gateway) OnPing(socket *gws.Conn, payload []byte) {
	_, ok := socket.Session().Load("connId")
	if ok {
		_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
		_ = socket.WritePong([]byte{})
	}
}

func (c *Gateway) OnPong(socket *gws.Conn, payload []byte) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
}

func (c *Gateway) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()
	defer c.recover("OnMessage", socket)
	connId, ok := socket.Session().Load("connId")
	switch ok {
	case false:
		by, _, err := c.Fn("", message.Bytes())
		if err != nil {
			defer c.ServerOnClose(socket, err)
			return
		}
		sn := c.extractSn(by)
		if sn != "" {
			socket.Session().Store("connId", sn)
			c.connection.Storage(sn, socket)
		}
		socket.WriteMessage(message.Opcode, by)
	case true:
		by, sync, err := c.Fn(connId.(string), message.Bytes())
		if err != nil {
			defer socket.WriteMessage(message.Opcode, []byte(err.Error()))
			return
		}
		if sync && len(by) > 0 {
			socket.WriteMessage(message.Opcode, by)
		}

	}
}

func (c *Gateway) recover(ctx string, socket *gws.Conn, err ...interface{}) {
	if r := recover(); r != nil {
		c.Logger.Errorf("[Recover] %s panic: %v, stack: %s", ctx, r, debug.Stack())
		socket.WriteMessage(gws.OpcodeText, serverError)
	}
}

func (c *Gateway) extractSn(payload []byte) string {
	key := []byte(`"connId":"`)
	i := bytes.Index(payload, key)
	if i == -1 {
		return ""
	}
	start := i + len(key)
	end := bytes.IndexByte(payload[start:], '"')
	if end == -1 {
		return ""
	}
	return string(payload[start : start+end])
}

// SetKeepAlive 从 net.Conn 中提取 TCP 连接并设置 keep-alive。
func SetKeepAlive(conn net.Conn) error {
	var tcpConn *net.TCPConn

	// 逐层解包，直到找到 *net.TCPConn
	switch c := conn.(type) {
	case *net.TCPConn:
		tcpConn = c
	case *tls.Conn:
		// Go 1.18+ 提供了 NetConn()，直接取下层连接
		if nc, ok := c.NetConn().(*net.TCPConn); ok {
			tcpConn = nc
		}
	}

	if tcpConn == nil {
		return fmt.Errorf("connection is not a TCP connection (got %T)", conn)
	}

	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	// 设置空闲后多久开始发送探测包
	return tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     60 * time.Second,
		Interval: 15 * time.Second,
		Count:    3,
	})
}
