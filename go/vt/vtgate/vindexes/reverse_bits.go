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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn = (*ReverseBits)(nil)
	_ Reversible   = (*ReverseBits)(nil)
)

// ReverseBits defines vindex that reverses the bits of a number.
// It's Unique, Reversible and Functional.
type ReverseBits struct {
	name string
}

// NewReverseBits creates a new ReverseBits.
func NewReverseBits(name string, m map[string]string) (Vindex, error) {
	return &ReverseBits{name: name}, nil
}

// String returns the name of the vindex.
func (vind *ReverseBits) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *ReverseBits) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *ReverseBits) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *ReverseBits) NeedsVCursor() bool {
	return false
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *ReverseBits) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		num, err := evalengine.ToUint64(id)
		if err != nil {
			out[i] = key.DestinationNone{}
			continue
		}
		out[i] = key.DestinationKeyspaceID(reverse(num))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *ReverseBits) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := evalengine.ToUint64(ids[i])
		if err != nil {
			return nil, err
		}
		out[i] = bytes.Equal(reverse(num), ksids[i])
	}
	return out, nil
}

// ReverseMap returns the ids from ksids.
func (vind *ReverseBits) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	reverseIds := make([]sqltypes.Value, 0, len(ksids))
	for _, keyspaceID := range ksids {
		val, err := unreverse(keyspaceID)
		if err != nil {
			return reverseIds, err
		}
		reverseIds = append(reverseIds, sqltypes.NewUint64(val))
	}
	return reverseIds, nil
}

func init() {
	Register("reverse_bits", NewReverseBits)
}

func reverse(shardKey uint64) []byte {
	reversed := make([]byte, 8)
	binary.BigEndian.PutUint64(reversed, bits.Reverse64(shardKey))
	return reversed
}

func unreverse(k []byte) (uint64, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("invalid keyspace id: %v", hex.EncodeToString(k))
	}
	return bits.Reverse64(binary.BigEndian.Uint64(k)), nil
}
