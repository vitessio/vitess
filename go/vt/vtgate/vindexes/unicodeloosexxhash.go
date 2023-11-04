/*
Copyright 2020 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn    = (*UnicodeLooseXXHash)(nil)
	_ Hashing         = (*UnicodeLooseXXHash)(nil)
	_ ParamValidating = (*UnicodeLooseXXHash)(nil)
)

// UnicodeLooseXXHash is a vindex that normalizes and hashes unicode strings
// to a keyspace id. It conservatively converts the string to its base
// characters before hashing. This is also known as UCA level 1.
// Ref: http://www.unicode.org/reports/tr10/#Multi_Level_Comparison.
// This is compatible with MySQL's utf8_unicode_ci collation.
type UnicodeLooseXXHash struct {
	name          string
	unknownParams []string
}

// newUnicodeLooseXXHash creates a new UnicodeLooseXXHash struct.
func newUnicodeLooseXXHash(name string, m map[string]string) (Vindex, error) {
	return &UnicodeLooseXXHash{
		name:          name,
		unknownParams: FindUnknownParams(m, nil),
	}, nil
}

// String returns the name of the vindex.
func (vind *UnicodeLooseXXHash) String() string {
	return vind.name
}

// Cost returns the cost as 1.
func (vind *UnicodeLooseXXHash) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *UnicodeLooseXXHash) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *UnicodeLooseXXHash) NeedsVCursor() bool {
	return false
}

// Verify returns true if ids maps to ksids.
func (vind *UnicodeLooseXXHash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		data, err := vind.Hash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseXXHash.Verify: %v", err)
		}
		out = append(out, bytes.Equal(data, ksids[i]))
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *UnicodeLooseXXHash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		data, err := vind.Hash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseXXHash.Map: %v", err)
		}
		out = append(out, key.DestinationKeyspaceID(data))
	}
	return out, nil
}

func (vind *UnicodeLooseXXHash) Hash(id sqltypes.Value) ([]byte, error) {
	return unicodeHash(vXXHash, id)
}

// UnknownParams implements the ParamValidating interface.
func (vind *UnicodeLooseXXHash) UnknownParams() []string {
	return vind.unknownParams
}

func init() {
	Register("unicode_loose_xxhash", newUnicodeLooseXXHash)
}
