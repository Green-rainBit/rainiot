package cache

var (
	WsBalancedPublishCache = "publish:iot:ws:balanced:"
)

func GetWsBalancedPublishCache(serverName string) string {
	return GetCacheKey(WsBalancedPublishCache + serverName)
}

type WsBalancedPublish struct {
	Meanws int64

	ReceiveWsserver       []string
	ReceiveWsserverAmount []int64
}
