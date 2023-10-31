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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn    = (*UnicodeLooseMD5)(nil)
	_ Hashing         = (*UnicodeLooseMD5)(nil)
	_ ParamValidating = (*UnicodeLooseMD5)(nil)
)

// UnicodeLooseMD5 is a vindex that normalizes and hashes unicode strings
// to a keyspace id. It conservatively converts the string to its base
// characters before hashing. This is also known as UCA level 1.
// Ref: http://www.unicode.org/reports/tr10/#Multi_Level_Comparison.
// This is compatible with MySQL's utf8_unicode_ci collation.
type UnicodeLooseMD5 struct {
	name          string
	unknownParams []string
}

// newUnicodeLooseMD5 creates a new UnicodeLooseMD5.
func newUnicodeLooseMD5(name string, m map[string]string) (Vindex, error) {
	return &UnicodeLooseMD5{
		name:          name,
		unknownParams: FindUnknownParams(m, nil),
	}, nil
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
func (vind *UnicodeLooseMD5) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, 0, len(ids))
	for i, id := range ids {
		data, err := vind.Hash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Verify: %v", err)
		}
		out = append(out, bytes.Equal(data, ksids[i]))
	}
	return out, nil
}

// Map can map ids to key.Destination objects.
func (vind *UnicodeLooseMD5) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		data, err := vind.Hash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Map: %v", err)
		}
		out = append(out, key.DestinationKeyspaceID(data))
	}
	return out, nil
}

func (vind *UnicodeLooseMD5) Hash(id sqltypes.Value) ([]byte, error) {
	return unicodeHash(&collateMD5, id)
}

// UnknownParams implements the ParamValidating interface.
func (vind *UnicodeLooseMD5) UnknownParams() []string {
	return vind.unknownParams
}

func init() {
	Register("unicode_loose_md5", newUnicodeLooseMD5)
}
