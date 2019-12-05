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

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ Vindex = (*XXHash)(nil)
)

// XXHash defines vindex that hashes any sql types to a KeyspaceId
// by using xxhash64. It's Unique and works on any platform giving identical result.
type XXHash struct {
	name string
}

// NewXXHash creates a new XXHash.
func NewXXHash(name string, m map[string]string) (Vindex, error) {
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

// Map can map ids to key.Destination objects.
func (vind *XXHash) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i := range ids {
		id := ids[i].ToBytes()
		out[i] = key.DestinationKeyspaceID(vXXHash(id))
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *XXHash) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		id := ids[i].ToBytes()
		out[i] = bytes.Equal(vXXHash(id), ksids[i])
	}
	return out, nil
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
