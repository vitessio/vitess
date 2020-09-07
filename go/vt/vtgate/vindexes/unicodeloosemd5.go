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
	_ SingleColumn = (*UnicodeLooseMD5)(nil)
)

// UnicodeLooseMD5 is a vindex that normalizes and hashes unicode strings
// to a keyspace id. It conservatively converts the string to its base
// characters before hashing. This is also known as UCA level 1.
// Ref: http://www.unicode.org/reports/tr10/#Multi_Level_Comparison.
// This is compatible with MySQL's utf8_unicode_ci collation.
type UnicodeLooseMD5 struct {
	name string
}

// NewUnicodeLooseMD5 creates a new UnicodeLooseMD5.
func NewUnicodeLooseMD5(name string, _ map[string]string) (Vindex, error) {
	return &UnicodeLooseMD5{name: name}, nil
}

// String returns the name of the vindex.
func (vind *UnicodeLooseMD5) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *UnicodeLooseMD5) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *UnicodeLooseMD5) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *UnicodeLooseMD5) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids maps to ksids.
func (vind *UnicodeLooseMD5) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		data, err := unicodeHash(vMD5Hash, ids[i])
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Verify: %v", err)
		}
		out[i] = bytes.Equal(data, ksids[i])
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *UnicodeLooseMD5) Map(cursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		data, err := unicodeHash(vMD5Hash, id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Map: %v", err)
		}
		out = append(out, key.DestinationKeyspaceID(data))
	}
	return out, nil
}

func init() {
	Register("unicode_loose_md5", NewUnicodeLooseMD5)
}
