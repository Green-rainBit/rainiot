package ctypes

import "encoding/json"

type Login struct {
	InstanceId string `redis:"instanceId"`
	Addrs      string `redis:"addrs"`
	IsEncrypt  bool   `redis:"isEncrypt"`
}

func (l Login) MarshalBinary() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Login) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, l)
}

type WsBalancedPublish struct {
	Meanws int64

	ReceiveWsserver       []string
	ReceiveWsserverAmount []int64
}
