package json2

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/hack"
)

func init() {
	hack.DisableProtoBufRandomness()
}

// MarshalPB marshals a proto.
func MarshalPB(pb proto.Message) ([]byte, error) {
	return protojson.Marshal(pb)
}

// MarshalIndentPB MarshalIndents a proto.
func MarshalIndentPB(pb proto.Message, indent string) ([]byte, error) {
	return protojson.MarshalOptions{
		Multiline: true,
		Indent:    indent,
	}.Marshal(pb)
}
