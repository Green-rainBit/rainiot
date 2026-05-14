package cache

import "time"

const (
	cacheConn             = "conn:"
	cacheKeyPrefix        = "iot:cache:"
	cacheWsServerName     = "iot:ws:server:"
	cacheWsConn           = "iot:ws:conn:"
	cacheWsBalancedLastat = "iot:ws:balance:lastat"

	LockTimne = 10 * time.Second
)

func GetCacheConn(key string) string {
	return cacheConn + key
}

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

func GetCacheWsBalancedLastat() string {
	return cacheWsBalancedLastat
}
