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
	"crypto/md5"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ Functional = (*BinaryMD5)(nil)
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

// Verify returns true if ids maps to ksids.
func (vind *BinaryMD5) Verify(v VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(rowsColValues))
	for idx, ids := range rowsColValues {
		ksid := make([]byte, 0)
		for _, id := range ids {
			ksid = append(ksid, binHash(id.ToBytes())...)
		}
		out[idx] = (bytes.Compare(ksid, ksids[idx]) == 0)
	}
	return out, nil
}

// Map returns the corresponding keyspace id values for the given ids.
func (vind *BinaryMD5) Map(_ VCursor, rowsColValues [][]sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, len(rowsColValues))
	for idx, ids := range rowsColValues {
		for _, id := range ids {
			out[idx] = append(out[idx], binHash(id.ToBytes())...)
		}
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
