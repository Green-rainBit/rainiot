package model

import (
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ DeviceModel = (*customDeviceModel)(nil)

type (
	// DeviceModel is an interface to be customized, add more methods here,
	// and implement the added methods in customDeviceModel.
	DeviceModel interface {
		deviceModel
	}

	customDeviceModel struct {
		*defaultDeviceModel
	}
)

// NewDeviceModel returns a model for the database table.
func NewDeviceModel(conn sqlx.SqlConn, rds *redis.Redis, opts ...cache.Option) DeviceModel {
	return &customDeviceModel{
		defaultDeviceModel: newDeviceModel(conn, rds, opts...),
	}
}
