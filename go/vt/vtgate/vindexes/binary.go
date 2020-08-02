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

package vindexes

import (
	"bytes"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn = (*Binary)(nil)
	_ Reversible   = (*Binary)(nil)
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
	return 0
}

// IsUnique returns true since the Vindex is unique.
func (vind *Binary) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *Binary) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids maps to ksids.
func (vind *Binary) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		out[i] = bytes.Equal(ids[i].ToBytes(), ksids[i])
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *Binary) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		out[i] = key.DestinationKeyspaceID(id.ToBytes())
	}
	return out, nil
}

// ReverseMap returns the associated ids for the ksids.
func (*Binary) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	var reverseIds = make([]sqltypes.Value, len(ksids))
	for rownum, keyspaceID := range ksids {
		if keyspaceID == nil {
			return nil, fmt.Errorf("Binary.ReverseMap: keyspaceId is nil")
		}
		reverseIds[rownum] = sqltypes.MakeTrusted(sqltypes.VarBinary, keyspaceID)
	}
	return reverseIds, nil
}

func init() {
	Register("binary", NewBinary)
}
