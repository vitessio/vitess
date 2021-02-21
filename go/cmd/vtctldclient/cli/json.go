/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cli

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

// MarshalJSON marshals obj to a JSON string. It uses the jsonpb marshaler for
// proto.Message types, with some sensible defaults, and falls back to the
// standard Go marshaler otherwise. In both cases, the marshaled JSON is
// indented with two spaces for readability.
//
// Unfortunately jsonpb only works for types that implement proto.Message,
// either by being a proto message type or by anonymously embedding one, so for
// other types that may have nested struct fields, we still use the standard Go
// marshaler, which will result in different formattings.
func MarshalJSON(obj interface{}) ([]byte, error) {
	switch obj := obj.(type) {
	case proto.Message:
		b := bytes.NewBuffer(nil)
		m := jsonpb.Marshaler{
			EnumsAsInts:  false,
			EmitDefaults: true,
			Indent:       "  ",
			OrigName:     true,
		}

		if err := m.Marshal(b, obj); err != nil {
			return nil, fmt.Errorf("jsonpb.Marshal = %v", err)
		}

		return b.Bytes(), nil
	default:
		data, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("json.Marshal = %v", err)
		}

		return data, nil
	}
}
