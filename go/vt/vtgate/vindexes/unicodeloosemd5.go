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
	"fmt"
	"sync"
	"unicode/utf8"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

var (
	_ Vindex = (*UnicodeLooseMD5)(nil)
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

// IsFunctional returns true since the Vindex is functional.
func (vind *UnicodeLooseMD5) IsFunctional() bool {
	return true
}

// Verify returns true if ids maps to ksids.
func (vind *UnicodeLooseMD5) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		data, err := unicodeHash(ids[i])
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
		data, err := unicodeHash(id)
		if err != nil {
			return nil, fmt.Errorf("UnicodeLooseMD5.Map: %v", err)
		}
		out = append(out, key.DestinationKeyspaceID(data))
	}
	return out, nil
}

func unicodeHash(key sqltypes.Value) ([]byte, error) {
	collator := collatorPool.Get().(pooledCollator)
	defer collatorPool.Put(collator)

	norm, err := normalize(collator.col, collator.buf, key.ToBytes())
	if err != nil {
		return nil, err
	}
	return binHash(norm), nil
}

func normalize(col *collate.Collator, buf *collate.Buffer, in []byte) ([]byte, error) {
	// We cannot pass invalid UTF-8 to the collator.
	if !utf8.Valid(in) {
		return nil, fmt.Errorf("cannot normalize string containing invalid UTF-8: %q", string(in))
	}

	// Ref: http://dev.mysql.com/doc/refman/5.6/en/char.html.
	// Trailing spaces are ignored by MySQL.
	in = bytes.TrimRight(in, " ")

	// We use the collation key which can be used to
	// perform lexical comparisons.
	return col.Key(buf, in), nil
}

// pooledCollator pairs a Collator and a Buffer.
// These pairs are pooled to avoid reallocating for every request,
// which would otherwise be required because they can't be used concurrently.
//
// Note that you must ensure no active references into the buffer remain
// before you return this pair back to the pool.
// That is, either do your processing on the result first, or make a copy.
type pooledCollator struct {
	col *collate.Collator
	buf *collate.Buffer
}

var collatorPool = sync.Pool{New: newPooledCollator}

func newPooledCollator() interface{} {
	// Ref: http://www.unicode.org/reports/tr10/#Introduction.
	// Unicode seems to define a universal (or default) order.
	// But various locales have conflicting order,
	// which they have the right to override.
	// Unfortunately, the Go library requires you to specify a locale.
	// So, I chose English assuming that it won't override
	// the Unicode universal order. But I couldn't find an easy
	// way to verify this.
	// Also, the locale differences are not an issue for level 1,
	// because the conservative comparison makes them all equal.
	return pooledCollator{
		col: collate.New(language.English, collate.Loose),
		buf: new(collate.Buffer),
	}
}

func init() {
	Register("unicode_loose_md5", NewUnicodeLooseMD5)
}
