package ws

import (
	"sync"
	"sync/atomic"
)

type Connection interface {
	Store(connId string, conn any) (old any, replaced bool)
	Delete(connId string, conn any) bool // 只有 map 里仍是这个 socket 才删
	GetConnByConnId(connId string) (any, bool)
	GetConnByCount() int64
	Take(n int64) []any
	Range(func(key, value any) bool)
}

type connection struct {
	soketMap sync.Map
	count    int64
}

func NewConnection() Connection {
	return &connection{
		soketMap: sync.Map{},
		count:    0,
	}
}

func (c *connection) Store(connId string, conn any) (old any, replaced bool) {
	old, replaced = c.soketMap.Swap(connId, conn)
	if !replaced {
		atomic.AddInt64(&c.count, 1)
	}

	return old, replaced
}

func (c *connection) Delete(connId string, conn any) bool {
	if c.soketMap.CompareAndDelete(connId, conn) {
		atomic.AddInt64(&c.count, -1)
		return true
	}
	return false
}

func (c *connection) GetConnByConnId(connId string) (any, bool) {
	return c.soketMap.Load(connId)
}

func (c *connection) GetConnByCount() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *connection) Take(n int64) []any {
	if n <= 0 {
		return nil
	}
	if max := c.GetConnByCount(); n > max {
		n = max
	}
	conns := make([]any, 0, n)
	c.soketMap.Range(func(_, v any) bool {
		conns = append(conns, v)
		return int64(len(conns)) < n
	})
	return conns
}

func (c *connection) Range(f func(key, value any) bool) {
	c.soketMap.Range(f)
}
