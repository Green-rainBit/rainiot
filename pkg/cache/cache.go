package cache

import (
	"context"
)

type Login struct {
	ServiceName string `json:"serviceName"`
	IsEncrypt   bool   `json:"isEncrypt"`
}

type CacheCli interface {
	SetLoginBySn(ctx context.Context, sn string, cacheLogin Login) error
	GetLoginBySn(ctx context.Context, sn string) (Login, bool, error)
	Exists(ctx context.Context, key ...string) (int64, error)
	Publish(ctx context.Context, channel string, message interface{}) error
}
