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
	"context"
	"encoding/binary"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn    = (*Numeric)(nil)
	_ Reversible      = (*Numeric)(nil)
	_ Hashing         = (*Numeric)(nil)
	_ ParamValidating = (*Numeric)(nil)
	_ Sequential      = (*Numeric)(nil)
)

// Numeric defines a bit-pattern mapping of a uint64 to the KeyspaceId.
// It's Unique and Reversible.
type Numeric struct {
	name          string
	unknownParams []string
}

// newNumeric creates a Numeric vindex.
func newNumeric(name string, m map[string]string) (Vindex, error) {
	return &Numeric{
		name:          name,
		unknownParams: FindUnknownParams(m, nil),
	}, nil
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

// NeedsVCursor satisfies the Vindex interface.
func (*Numeric) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids and ksids match.
func (vind *Numeric) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes.Equal(ksid, ksids[i]))
	}
	return out, nil
}

// Map can map ids to key.ShardDestination objects.
func (vind *Numeric) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.ShardDestination, error) {
	out := make([]key.ShardDestination, 0, len(ids))
	for _, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			out = append(out, key.DestinationNone{})
			continue
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
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
		val := binary.BigEndian.Uint64(keyspaceID)
		reverseIds[i] = sqltypes.NewUint64(val)
	}
	return reverseIds, nil
}

// RangeMap implements Between.
func (vind *Numeric) RangeMap(ctx context.Context, vcursor VCursor, startId sqltypes.Value, endId sqltypes.Value) ([]key.ShardDestination, error) {
	startKsId, err := vind.Hash(startId)
	if err != nil {
		return nil, err
	}
	endKsId, err := vind.Hash(endId)
	if err != nil {
		return nil, err
	}
	out := []key.ShardDestination{&key.DestinationKeyRange{KeyRange: key.NewKeyRange(startKsId, endKsId)}}
	return out, nil
}

// UnknownParams implements the ParamValidating interface.
func (vind *Numeric) UnknownParams() []string {
	return vind.unknownParams
}

func (*Numeric) Hash(id sqltypes.Value) ([]byte, error) {
	num, err := id.ToCastUint64()
	if err != nil {
		return nil, err
	}
	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], num)
	return keybytes[:], nil
}

func init() {
	Register("numeric", newNumeric)
}
