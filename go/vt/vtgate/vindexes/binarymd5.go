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
	"crypto/md5"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn    = (*BinaryMD5)(nil)
	_ Hashing         = (*BinaryMD5)(nil)
	_ ParamValidating = (*BinaryMD5)(nil)
)

// BinaryMD5 is a vindex that hashes binary bits to a keyspace id.
type BinaryMD5 struct {
	name          string
	unknownParams []string
}

// newBinaryMD5 creates a new BinaryMD5.
func newBinaryMD5(name string, params map[string]string) (Vindex, error) {
	return &BinaryMD5{
		name:          name,
		unknownParams: FindUnknownParams(params, nil),
	}, nil
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

// NeedsVCursor satisfies the Vindex interface.
func (vind *BinaryMD5) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids maps to ksids.
func (vind *BinaryMD5) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
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

// Map can map ids to key.Destination objects.
func (vind *BinaryMD5) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			return out, err
		}
		out = append(out, key.DestinationKeyspaceID(ksid))
	}
	return out, nil
}

func (vind *BinaryMD5) Hash(id sqltypes.Value) ([]byte, error) {
	idBytes, err := id.ToBytes()
	if err != nil {
		return nil, err
	}
	return vMD5Hash(idBytes), nil
}

// UnknownParams implements the ParamValidating interface.
func (vind *BinaryMD5) UnknownParams() []string {
	return vind.unknownParams
}

func vMD5Hash(source []byte) []byte {
	sum := md5.Sum(source)
	return sum[:]
}

func init() {
	Register("binary_md5", newBinaryMD5)
}
