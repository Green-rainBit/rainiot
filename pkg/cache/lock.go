package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	maxWaitTime   = 5 * time.Second
	retryInterval = 100 * time.Millisecond
)

func Lock(cli *redis.ClusterClient, ctx context.Context, lockKey string) error {
	err := cli.SetArgs(ctx, lockKey, "", redis.SetArgs{
		Mode: "NX",
		TTL:  LockTimne,
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

// BlockUntilLock 阻塞地获取分布式锁，直到成功或超时
func BlockUntilLock(cli *redis.ClusterClient, ctx context.Context, lockKey string) error {

	// 创建一个带超时的context，用于控制最大等待时间
	timeoutCtx, cancel := context.WithTimeout(ctx, maxWaitTime)
	defer cancel()

	for {
		// 1. 尝试获取锁
		err := Lock(cli, timeoutCtx, lockKey)
		if err == nil {
			return nil // 成功获取锁
		}

		// 2. 检查是否因为超时而失败
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("failed to acquire lock within %v: %w", maxWaitTime, err)
		}

		// 3. 还未超时，等待一段时间后继续重试
		select {
		case <-timeoutCtx.Done():
			// 如果在等待重试时，整个超时时间到了，则返回失败
			return fmt.Errorf("waiting for lock interrupted: %w", timeoutCtx.Err())
		case <-time.After(retryInterval):
			// 等待retryInterval后，继续循环重试
		}
	}
}

func Unlock(cli *redis.ClusterClient, ctx context.Context, key string) error {
	_, err := cli.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	return nil
}
