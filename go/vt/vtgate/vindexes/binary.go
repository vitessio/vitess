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

package vindexes

import (
	"bytes"
	"fmt"
)

// Binary is a vindex that converts binary bits to a keyspace id.
type Binary struct {
	name string
}

// NewBinary creates a new Binary.
func NewBinary(name string, _ map[string]string) (Vindex, error) {
	return &Binary{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Binary) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *Binary) Cost() int {
	return 1
}

// Verify returns true if ids maps to ksids.
func (vind *Binary) Verify(_ VCursor, ids []interface{}, ksids [][]byte) (bool, error) {
	if len(ids) != len(ksids) {
		return false, fmt.Errorf("Binary.Verify: length of ids %v doesn't match length of ksids %v", len(ids), len(ksids))
	}
	for rowNum := range ids {
		data, err := getBytes(ids[rowNum])
		if err != nil {
			return false, fmt.Errorf("Binary.Verify: %v", err)
		}
		if bytes.Compare(data, ksids[rowNum]) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *Binary) Map(_ VCursor, ids []interface{}) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		data, err := getBytes(id)
		if err != nil {
			return nil, fmt.Errorf("Binary.Map :%v", err)
		}
		out = append(out, data)
	}
	return out, nil
}

// ReverseMap returns the associated ids for the ksids.
func (*Binary) ReverseMap(_ VCursor, ksids [][]byte) ([]interface{}, error) {
	var reverseIds = make([]interface{}, len(ksids))
	for rownum, keyspaceID := range ksids {
		if keyspaceID == nil {
			return nil, fmt.Errorf("Binary.ReverseMap: keyspaceId is nil")
		}
		reverseIds[rownum] = []byte(keyspaceID)
	}
	return reverseIds, nil
}

func init() {
	Register("binary", NewBinary)
}
