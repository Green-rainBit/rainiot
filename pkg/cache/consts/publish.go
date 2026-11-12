package consts

import "regexp"

var (
	WsBalancedPublishCache = "ws:balanced:publish:"
	TaskTypeWsBalance      = "iot:cache:"
	queueNameRe            = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func GetWsBalancedPublishCache(serverName string) string {
	return GetCacheKey(WsBalancedPublishCache + serverName)
}

func WsBalanceQueue(instanceId string) string {
	return "ws-" + queueNameRe.ReplaceAllString(instanceId, "-")
}
