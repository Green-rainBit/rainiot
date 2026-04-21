package svc

import (
	"rainiot/iotws/internal/config"
	"sync"
	"sync/atomic"
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

// func (c *connection) NumberCorrection() {
// 	c.soketMap.Range(func(key any, value any) bool {

// 		return true
// 	})
// }
