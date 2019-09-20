/*
Copyright 2019 The Vitess Authors.

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

package json2

import (
	"bytes"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

// MarshalPB marshals a proto.
func MarshalPB(pb proto.Message) ([]byte, error) {
	buf := new(bytes.Buffer)
	m := jsonpb.Marshaler{}
	if err := m.Marshal(buf, pb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalIndentPB MarshalIndents a proto.
func MarshalIndentPB(pb proto.Message, indent string) ([]byte, error) {
	buf := new(bytes.Buffer)
	m := jsonpb.Marshaler{
		Indent: indent,
	}
	if err := m.Marshal(buf, pb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
