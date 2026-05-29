package ws

import (
	"sync"
	"sync/atomic"
)

type Connection interface {
	Storage(connId string, conn any) error
	Del(connId string)
	GetconnByConnId(connId string) (any, bool)
	GetNumber() int64
	GetConnByCount(count int64) ([]any, bool)
	Range(f func(key, value any) bool)
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

func (c *connection) Storage(connId string, conn any) error {
	c.soketMap.Store(connId, conn)
	atomic.AddInt64(&c.count, 1)
	return nil
}

func (c *connection) Del(connId string) {
	atomic.AddInt64(&c.count, -1)
	c.soketMap.Delete(connId)
}

func (c *connection) GetconnByConnId(connId string) (any, bool) {
	return c.soketMap.Load(connId)
}

func (c *connection) GetNumber() int64 {
	return c.count
}

func (c *connection) GetConnByCount(count int64) ([]any, bool) {
	conns := make([]any, count)
	i := int64(0)
	c.soketMap.Range(func(key, conn any) bool {
		i++
		if i >= count {
			return false
		}
		conns[i] = conn
		return true
	})
	return conns, true
}

func (c *connection) Range(f func(key, value any) bool) {
	c.soketMap.Range(f)
}
