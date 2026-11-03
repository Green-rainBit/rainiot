package wscli

type Request struct {
	ConnId   string `json:"ConnId"`
	MessgeId string `json:"messageId"`
	Data     []byte `json:"data"`
}
