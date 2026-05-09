package cache

const (
	cacheKeyPrefix    = "iot:cache:"
	cacheWsServerName = "iot:ws:server:"
	cacheWsConn       = "iot:ws:conn:"
)

func GetCacheKey(key string) string {
	return cacheKeyPrefix + key
}

func GetCacheWsConn() string {
	return cacheWsConn
}

func GetCacheWsServerName(id string) string {
	return cacheWsServerName + id
}
