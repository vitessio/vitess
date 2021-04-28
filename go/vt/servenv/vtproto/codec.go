package vtproto

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoimpl"
)

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func Marshal(v interface{}) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.MarshalVT()
	}
	return proto.Marshal(protoimpl.X.ProtoMessageV2Of(v))
}

func Unmarshal(data []byte, v interface{}) error {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.UnmarshalVT(data)
	}
	return proto.Unmarshal(data, protoimpl.X.ProtoMessageV2Of(v))
}

func JSONMarshal(msg interface{}) ([]byte, error) {
	return protojson.Marshal(protoimpl.X.ProtoMessageV2Of(msg))
}

func JSONUnmarshal(buf []byte, msg interface{}) error {
	return protojson.Unmarshal(buf, protoimpl.X.ProtoMessageV2Of(msg))
}
