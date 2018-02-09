/*
Copyright 2017 Google Inc.

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

package vindexes

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ Functional = (*Numeric)(nil)
	_ Reversible = (*Numeric)(nil)
)

// Numeric defines a bit-pattern mapping of a uint64 to the KeyspaceId.
// It's Unique and Reversible.
type Numeric struct {
	name string
}

// NewNumeric creates a Numeric vindex.
func NewNumeric(name string, _ map[string]string) (Vindex, error) {
	return &Numeric{name: name}, nil
}

// String returns the name of the vindex.
func (vind *Numeric) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 0.
func (*Numeric) Cost() int {
	return 0
}

// Verify returns true if ids and ksids match.
func (vind *Numeric) Verify(v VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(rowsColValues))
	for idx, ids := range rowsColValues {
		var kid []byte
		for _, id := range ids {
			var keybytes [8]byte
			num, err := sqltypes.ToUint64(id)
			if err != nil {
				if err != nil {
					return nil, fmt.Errorf("Numeric.Verify: %v", err)
				}
			}
			binary.BigEndian.PutUint64(keybytes[:], num)
			kid = append(kid, keybytes[:]...)
		}
		out[idx] = (bytes.Compare(kid, ksids[idx]) == 0)
	}
	return out, nil
}

// Map returns the associated keyspace ids for the given ids.
func (*Numeric) Map(_ VCursor, rowsColValues [][]sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, len(rowsColValues))
	for idx, ids := range rowsColValues {
		for _, id := range ids {
			num, err := sqltypes.ToUint64(id)
			if err != nil {
				out[idx] = nil
				continue
			}
			var keybytes [8]byte
			binary.BigEndian.PutUint64(keybytes[:], num)
			out[idx] = append(out[idx], keybytes[:]...)
		}
	}
	return out, nil
}

// ReverseMap returns the associated ids for the ksids.
func (*Numeric) ReverseMap(_ VCursor, ksids [][]byte) ([][]sqltypes.Value, error) {
	var reverseIds = make([][]sqltypes.Value, len(ksids))
	for idx, keyspaceID := range ksids {
		if len(keyspaceID)%8 != 0 {
			return nil, fmt.Errorf("Numeric.ReverseMap: length of keyspaceId is not valid: %d", len(keyspaceID))
		}
		for i := 0; i < len(keyspaceID); i += 8 {
			end := i + 8
			val := binary.BigEndian.Uint64([]byte(keyspaceID[i:end]))
			reverseIds[idx] = append(reverseIds[idx], sqltypes.NewUint64(val))
		}
	}
	return reverseIds, nil
}

func init() {
	Register("numeric", NewNumeric)
}
