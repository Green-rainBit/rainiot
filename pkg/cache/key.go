package cache

const (
	cacheKeyPrefix    = "iot:cache:"
	cacheWsServerName = "iot:ws:server"
	cacheWsConn       = "iot:ws:conn"
)

func GetCacheKey(key string) string {
	return cacheKeyPrefix + key
}

func GetCacheWsConn() string {
	return cacheWsConn
}
