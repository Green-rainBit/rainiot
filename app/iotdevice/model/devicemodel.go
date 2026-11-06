package model

import (
	"context"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ DeviceModel = (*customDeviceModel)(nil)

type (
	// DeviceModel is an interface to be customized, add more methods here,
	// and implement the added methods in customDeviceModel.
	DeviceModel interface {
		deviceModel
		GetOneBySn(ctx context.Context, sn string) (*Device, bool, error)
	}

	customDeviceModel struct {
		*defaultDeviceModel
	}
)

// NewDeviceModel returns a model for the database table.
func NewDeviceModel(conn sqlx.SqlConn) DeviceModel {
	return &customDeviceModel{
		defaultDeviceModel: newDeviceModel(conn),
	}
}

func (m *customDeviceModel) GetOneBySn(ctx context.Context, sn string) (*Device, bool, error) {
	resp, err := m.FindOneBySn(ctx, sn)
	switch err {
	case nil:
		return resp, true, nil
	case ErrNotFound:
		return nil, false, nil
	default:
		return nil, false, err
	}
}
