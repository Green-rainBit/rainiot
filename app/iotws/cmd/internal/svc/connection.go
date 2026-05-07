package svc

import (
	"sync"
	"sync/atomic"

	"rainiot/app/iotws/cmd/internal/config"
)

type connection struct {
	soketMap sync.Map
	count    int64 // 原子计数器
}

func NewConnection(c config.Config) *connection {
	return &connection{
		soketMap: sync.Map{},
		count:    0, // 原子计数器
	}
}

func (c *connection) Storage(sn string, conn any) error {
	c.soketMap.Store(sn, conn)
	atomic.AddInt64(&c.count, 1)
	return nil
}

func (c *connection) Del(sn string) {
	atomic.AddInt64(&c.count, -1)
	c.soketMap.Delete(sn)
}

func (c *connection) Get(sn string) (any, bool) {
	return c.soketMap.Load(sn)
}

func (c *connection) GetNumber(sn string) int64 {
	return c.count
}
