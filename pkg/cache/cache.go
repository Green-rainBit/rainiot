package cache

import (
	"context"
	"encoding/json"
)

type Login struct {
	ServiceName string `json:"serviceName"`
	IsEncrypt   bool   `json:"isEncrypt"`
}


func (l Login) MarshalBinary() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Login) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, l)
}

type CacheCli interface {
	SetLoginBySn(ctx context.Context, sn string, cacheLogin Login) error
	GetLoginBySn(ctx context.Context, sn string) (Login, bool, error)
	Exists(ctx context.Context, key ...string) (int64, error)
	Publish(ctx context.Context, channel string, message interface{}) error
}
