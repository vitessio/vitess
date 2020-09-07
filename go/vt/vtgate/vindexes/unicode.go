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
	"fmt"
	"sync"
	"unicode/utf8"

	"vitess.io/vitess/go/sqltypes"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// Shared functions for Unicode string normalization
// for Vindexes.

func unicodeHash(hashFunc func([]byte) []byte, key sqltypes.Value) ([]byte, error) {
	collator := collatorPool.Get().(*pooledCollator)
	defer collatorPool.Put(collator)

	norm, err := normalize(collator.col, collator.buf, key.ToBytes())
	if err != nil {
		return nil, err
	}
	return hashFunc(norm), nil
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
	return &pooledCollator{
		col: collate.New(language.English, collate.Loose),
		buf: new(collate.Buffer),
	}
}
