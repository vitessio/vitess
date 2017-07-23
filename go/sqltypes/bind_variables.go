/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
