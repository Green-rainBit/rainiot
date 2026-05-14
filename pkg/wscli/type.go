package wscli

type Request struct {
	Id   string `json:"id"`
	Data []byte `json:"data"`
}

