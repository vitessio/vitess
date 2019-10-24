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
	"crypto/md5"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ Vindex = (*BinaryMD5)(nil)
)

// BinaryMD5 is a vindex that hashes binary bits to a keyspace id.
type BinaryMD5 struct {
	name string
}

// NewBinaryMD5 creates a new BinaryMD5.
func NewBinaryMD5(name string, _ map[string]string) (Vindex, error) {
	return &BinaryMD5{name: name}, nil
}

// String returns the name of the vindex.
func (vind *BinaryMD5) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *BinaryMD5) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *BinaryMD5) IsUnique() bool {
	return true
}

// IsFunctional returns true since the Vindex is functional.
func (vind *BinaryMD5) IsFunctional() bool {
	return true
}

// Verify returns true if ids maps to ksids.
func (vind *BinaryMD5) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		out[i] = bytes.Equal(binHash(ids[i].ToBytes()), ksids[i])
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *BinaryMD5) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, len(ids))
	for i, id := range ids {
		out[i] = key.DestinationKeyspaceID(binHash(id.ToBytes()))
	}
	return out, nil
}

func binHash(source []byte) []byte {
	sum := md5.Sum(source)
	return sum[:]
}

func init() {
	Register("binary_md5", NewBinaryMD5)
}
