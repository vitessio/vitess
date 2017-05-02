package sqltypes

import (
	"reflect"

	"github.com/golang/protobuf/proto"
)

// BindVariablesEqual compares two maps of bind variables.
// For protobuf messages we have to use "proto.Equal".
func BindVariablesEqual(x, y map[string]interface{}) bool {
	if len(x) != len(y) {
		return false
	}
	for k := range x {
		vx, vy := x[k], y[k]
		if reflect.TypeOf(vx) != reflect.TypeOf(vy) {
			return false
		}
		switch vx.(type) {
		case proto.Message:
			if !proto.Equal(vx.(proto.Message), vy.(proto.Message)) {
				return false
			}
		default:
			if !reflect.DeepEqual(vx, vy) {
				return false
			}
		}
	}
	return true
}
