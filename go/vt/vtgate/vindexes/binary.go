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

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ Functional = (*Binary)(nil)
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

// Verify returns true if rowsColValues maps to ksids.
func (vind *Binary) Verify(v VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(rowsColValues))
	mappedKids, err := vind.Map(v, rowsColValues)
	if err != nil {
		return out, err
	}
	for i := range ksids {
		out[i] = (bytes.Compare(ksids[i], mappedKids[i]) == 0)
	}
	return out, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *Binary) Map(_ VCursor, rowsColValues [][]sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, len(rowsColValues))
	for idx, rows := range rowsColValues {
		for _, id := range rows {
			out[idx] = append(out[idx], id.ToBytes()...)
		}
	}
	return out, nil
}

func init() {
	Register("binary", NewBinary)
}
