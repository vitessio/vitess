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

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn = (*XXHash)(nil)
	_ Hashing      = (*XXHash)(nil)
)

// XXHash defines vindex that hashes any sql types to a KeyspaceId
// by using xxhash64. It's Unique and works on any platform giving identical result.
type XXHash struct {
	name string
}

// NewXXHash creates a new XXHash.
func NewXXHash(name string, _ map[string]string) (Vindex, error) {
	return &XXHash{name: name}, nil
}

// String returns the name of the vindex.
func (vind *XXHash) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *XXHash) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *XXHash) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *XXHash) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.Destination objects.
func (vind *XXHash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return nil, err
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *XXHash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return out, err
		}
		out = append(out, bytes.Equal(ksid, ksids[i]))
	}
	return out, nil
}

func (vind *XXHash) Hash(id sqltypes.Value) ([]byte, error) {
	idBytes, err := id.ToBytes()
	if err != nil {
		return nil, err
	}
	return vXXHash(idBytes), nil
}

func init() {
	Register("xxhash", NewXXHash)
}

func vXXHash(shardKey []byte) []byte {
	var hashed [8]byte
	hashKey := xxhash.Sum64(shardKey)
	binary.LittleEndian.PutUint64(hashed[:], hashKey)
	return hashed[:]
}
