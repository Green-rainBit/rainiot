package consts

import "regexp"

var (
	TaskTypeWsBalance = "task:balance"
	iotTask           = "iot:task:"
	queueNameRe       = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func WsBalanceQueue(instanceId string) string {
	return "ws-" + queueNameRe.ReplaceAllString(instanceId, "-")
}

func GetTaskTypeWsBalance(event string) string {
	return TaskTypeWsBalance
}

func SubscribeIotTaskQueueAckByEventAndMsgId(event, messgId string) string {
	return iotTask + event + "Ack:" + messgId
}

func GetIotTaskQueueByEventAndMsgId(event, messgId string) string {
	return iotTask + event + messgId
}

func PublishByIotTaskQueueAndInstanceId(instanceId string) string {
	return iotTask + instanceId
}

func SubscribeByIotTaskQueueAndInstanceId(instanceId string) string {
	return iotTask + instanceId
}

func SubscribeByIotTaskOKMsgId(msgId string) string {
	return iotTask + msgId
}
