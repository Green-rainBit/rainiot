package types

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
)

type AckRequest struct {
	MessageId string `form:"messageId "`
}

type IotdeviceReq interface {
	Descriptor() ([]byte, []int)
	GetCmd() string
	GetData() *structpb.Struct
	GetSn() string
	GetConnId() string
	GetServiceName() string
	ProtoMessage()
	ProtoReflect() protoreflect.Message
	Reset()
	String() string
}

func (m *Request) Descriptor() ([]byte, []int) {
	return []byte(nil), []int{}
}

func (m *Request) GetCmd() string {
	return m.Cmd
}

func (m *Request) GetData() *structpb.Struct {
	s := &structpb.Struct{}
	if len(m.Data) == 0 {
		return s
	}
	if err := protojson.Unmarshal(m.Data, s); err != nil {
		return s
	}
	return s
}

func (m *Request) GetSn() string {
	return m.Sn
}

func (m *Request) GetConnId() string {
	return m.ConnId
}

func (m *Request) ProtoMessage() {
	return
}

func (m *Request) ProtoReflect() protoreflect.Message {
	return nil
}

func (m *Request) GetServiceName() string {
	return m.ServiceName
}

func (m *Request) String() string {
	bytes, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func (m *Request) Reset() {
	m.Cmd = ""
	m.Sn = ""
	m.Data = nil
	m.ConnId = ""
	m.ServiceName = ""
}
