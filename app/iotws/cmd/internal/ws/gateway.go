package ws

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"time"

	"rainiot/pkg/cache"

	"github.com/lxzan/gws"
	"github.com/redis/go-redis/v9"
)

const (
	PingInterval = 5 * time.Second
	PingWait     = 10 * time.Second
)

var serverError = []byte("server error")

func NewGatewayr(serverName string, conn Connection, redis *redis.ClusterClient) *Gateway {
	return &Gateway{
		connection: conn,
		redis:      redis,
		serverName: serverName,
	}
}

type Gateway struct {
	serverName string
	Fn         func(message []byte) (by []byte, err error)
	connection Connection
	redis      *redis.ClusterClient
}

func (c *Gateway) OnOpen(socket *gws.Conn) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	go func() {
		timeAfter := time.After(PingInterval)

		<-timeAfter
		connId, ok := socket.Session().Load("connId")
		if ok {
			switch coId := connId.(type) {
			case string:
				if coId == "" {
					c.ServerOnClose(socket, errors.New("timeout"))
				} else {
					c.connection.Storage(coId, socket)
					c.redis.SetXX(context.Background(), cache.GetCacheConn(coId), c.serverName, cache.ConnTime)
				}
				SetKeepAlive(socket.NetConn())
			default:

			}
		}
	}()
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
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
	_ = socket.WritePong([]byte{})
}

func (c *Gateway) OnPong(socket *gws.Conn, payload []byte) {
	_ = socket.SetDeadline(time.Now().Add(PingInterval + PingWait))
}

func (c *Gateway) OnMessage(socket *gws.Conn, message *gws.Message) {
	defer message.Close()
	defer c.recover("OnMessage", socket)

	by, err := c.Fn(message.Bytes())
	_, ok := socket.Session().Load("connId")
	if !ok {
		if err != nil {
			defer c.ServerOnClose(socket, err)
			return
		}
		socket.Session().Store("connId", c.extractSn(message.Bytes()))
		socket.WriteMessage(message.Opcode, by)
	} else {
		if err != nil {
			defer socket.WriteMessage(message.Opcode, []byte(err.Error()))
			return
		}
		socket.WriteMessage(message.Opcode, by)
	}

}

func (c *Gateway) recover(ctx string, socket *gws.Conn, err ...interface{}) {
	if r := recover(); r != nil {
		log.Printf("[Recover] %s panic: %v\n%s", ctx, r, debug.Stack())
		socket.WriteMessage(gws.OpcodeText, serverError)
	}
}
func (c *Gateway) extractSn(payload []byte) []byte {
	key := []byte(`"sn":"`)
	i := bytes.Index(payload, key)
	if i == -1 {
		return nil
	}
	start := i + len(key)
	end := bytes.IndexByte(payload[start:], '"')
	if end == -1 {
		return nil
	}
	return payload[start : start+end]
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
