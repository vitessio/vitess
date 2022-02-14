package servenv

import (
	"fmt"

	// use the original golang/protobuf package we can continue serializing
	// messages from our dependencies, particularly from etcd
	"github.com/golang/protobuf/proto" //nolint

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto" // nolint:revive
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type vtprotoCodec struct{}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func (vtprotoCodec) Marshal(v interface{}) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.MarshalVT()
	}

	vv, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
	}
	return proto.Marshal(vv)
}

func (vtprotoCodec) Unmarshal(data []byte, v interface{}) error {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.UnmarshalVT(data)
	}

	vv, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}
	return proto.Unmarshal(data, vv)
}

func (vtprotoCodec) Name() string {
	return Name
}

func init() {
	encoding.RegisterCodec(vtprotoCodec{})
}
