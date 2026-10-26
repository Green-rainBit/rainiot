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
	_, replaced := c.soketMap.Swap(connId, conn)
	if replaced {
		atomic.AddInt64(&c.count, -1)
		c.soketMap.Delete(connId)
	}
	return replaced

}

func (c *connection) GetConnByConnId(connId string) (any, bool) {
	return c.soketMap.Load(connId)
}

func (c *connection) GetConnByCount() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *connection) Take(count int64) []any {
	conns := make([]any, count)
	i := int64(0)
	c.soketMap.Range(func(key, conn any) bool {

		if i >= count {
			return false
		}
		conns[i] = conn
		i++
		return true
	})
	return conns
}

func (c *connection) Range(f func(key, value any) bool) {
	c.soketMap.Range(f)
}
