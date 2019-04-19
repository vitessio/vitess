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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	_ Vindex     = (*Numeric)(nil)
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

// IsUnique returns true since the Vindex is unique.
func (*Numeric) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (*Numeric) IsFunctional() bool {
	return true
}

// Verify returns true if ids and ksids match.
func (*Numeric) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		var keybytes [8]byte
		num, err := sqltypes.ToUint64(ids[i])
		if err != nil {
			return nil, vterrors.Wrap(err, "Numeric.Verify")
		}
		binary.BigEndian.PutUint64(keybytes[:], num)
		out[i] = bytes.Equal(keybytes[:], ksids[i])
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (*Numeric) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		num, err := sqltypes.ToUint64(id)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		var keybytes [8]byte
		binary.BigEndian.PutUint64(keybytes[:], num)
		out = append(out, key.DestinationKeyspaceID(keybytes[:]))
	}
	return out, nil
}

// ReverseMap returns the associated ids for the ksids.
func (*Numeric) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	var reverseIds = make([]sqltypes.Value, len(ksids))
	for i, keyspaceID := range ksids {
		if len(keyspaceID) != 8 {
			return nil, fmt.Errorf("Numeric.ReverseMap: length of keyspaceId is not 8: %d", len(keyspaceID))
		}
		val := binary.BigEndian.Uint64([]byte(keyspaceID))
		reverseIds[i] = sqltypes.NewUint64(val)
	}
	return reverseIds, nil
}

func init() {
	Register("numeric", NewNumeric)
}
