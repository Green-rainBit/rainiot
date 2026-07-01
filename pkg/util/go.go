package util

import (
	"context"

	"github.com/zeromicro/go-zero/core/logx"
)

func Go(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logx.WithContext(context.Background()).Error(err)
				
			}
		}()
		fn()
	}()

}
