package consts

import "time"

const (
	cacheConn              = "conn:"
	cacheKeyPrefix         = "iot:cache:"
	cacheWsServerName      = "iot:ws:server:"
	cacheWsConn            = "iot:ws:conn:"
	cacheWsBalancedLast    = "iot:ws:balance:last" // balance in time
	WsBalancedPublishCache = "ws:balanced:publish:"

	ConnTime = time.Minute * 6

	LockTimne = 10 * time.Second
)

func GetCacheConn(key string) string {
	return cacheConn + key
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

func GetCacheWsBalancedLast() string {
	return cacheWsBalancedLast
}

func GetWsBalancedPublishCache(serverName string) string {
	return GetCacheKey(WsBalancedPublishCache + serverName)
}
