package cache

import "regexp"

var (
	WsBalancedPublishCache = "publish:iot:ws:balanced:"
	TaskTypeWsBalance      = "ws:balance"
	queueNameRe            = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func GetWsBalancedPublishCache(serverName string) string {
	return GetCacheKey(WsBalancedPublishCache + serverName)
}

type WsBalancedPublish struct {
	Meanws int64

	ReceiveWsserver       []string
	ReceiveWsserverAmount []int64
}

func WsBalanceQueue(instanceId string) string {
	return "ws-" + queueNameRe.ReplaceAllString(instanceId, "-")
}
