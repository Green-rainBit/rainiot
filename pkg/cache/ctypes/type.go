package ctypes

import "encoding/json"

type Login struct {
	ServiceName string `json:"serviceName"`
	IsEncrypt   bool   `json:"isEncrypt"`
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
