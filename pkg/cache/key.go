package cache

import "time"

const (
	cacheKeyPrefix    = "iot:cache:"
	cacheWsServerName = "iot:ws:server:"
	cacheWsConn       = "iot:ws:conn:"

	LockTimne = 10 * time.Second
)

func CacheWsServerNameLock(key string) string {
	return GetCacheWsServerName(key) + ":lock"
}

func GetCacheKey(key string) string {
	return cacheKeyPrefix + key
}

func GetCacheWsConn() string {
	return cacheWsConn
}

func GetCacheWsServerName(id string) string {
	return cacheWsServerName + id
}
