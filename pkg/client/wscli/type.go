package wscli

type Request struct {
	ConnId   string `json:"connId"`
	MessgeId string `json:"messageId"`
	Data     []byte `json:"data"`
}
