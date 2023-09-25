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
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"google.golang.org/protobuf/proto"
)

const (
	jsonIndent = "  "
	jsonPrefix = ""
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
func MarshalJSON(obj any) ([]byte, error) {
	switch obj := obj.(type) {
	case proto.Message:
		m := protojson.MarshalOptions{
			Multiline:       true,
			Indent:          jsonIndent,
			UseEnumNumbers:  true,
			UseProtoNames:   true,
			EmitUnpopulated: true,
		}
		return m.Marshal(obj)
	default:
		data, err := json.MarshalIndent(obj, jsonPrefix, jsonIndent)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal = %v", err)
		}

		return data, nil
	}
}

// MarshalJSONPretty works the same as MarshalJSON but elides zero value
// elements and uses ENUM names instead of numbers.
func MarshalJSONPretty(obj any) ([]byte, error) {
	switch obj := obj.(type) {
	case proto.Message:
		m := protojson.MarshalOptions{
			Multiline:       true,
			Indent:          jsonIndent,
			UseEnumNumbers:  false,
			UseProtoNames:   true,
			EmitUnpopulated: false, // elide zero value elements
		}
		return m.Marshal(obj)
	default:
		data, err := json.MarshalIndent(obj, jsonPrefix, jsonIndent)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal = %v", err)
		}

		return data, nil
	}
}
