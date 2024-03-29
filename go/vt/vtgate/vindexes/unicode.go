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
	"crypto/md5"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/mysql/collations/vindex/collate"
	"vitess.io/vitess/go/sqltypes"
)

// Shared functions for Unicode string normalization
// for Vindexes.

func unicodeHash(pool *sync.Pool, key sqltypes.Value) ([]byte, error) {
	collator := pool.Get().(*collate.Hasher)
	defer pool.Put(collator)

	keyBytes, err := key.ToBytes()
	if err != nil {
		return nil, err
	}

	// We cannot pass invalid UTF-8 to the collator.
	if !utf8.Valid(keyBytes) {
		return nil, fmt.Errorf("cannot normalize string containing invalid UTF-8: %q", keyBytes)
	}

	// Ref: http://dev.mysql.com/doc/refman/5.6/en/char.html.
	// Trailing spaces are ignored by MySQL.
	keyBytes = bytes.TrimRight(keyBytes, " ")

	// We use the collation key which can be used to
	// perform lexical comparisons.
	return collator.Hash(keyBytes), nil
}

var collateMD5 = sync.Pool{New: func() any {
	return collate.New(md5.New())
}}

var collateXX = sync.Pool{New: func() any {
	return collate.New(XXHashBigEndian{Digest: xxhash.New()})
}}

type XXHashBigEndian struct {
	*xxhash.Digest
}

func (d XXHashBigEndian) Sum(b []byte) []byte {
	s := d.Sum64()
	return append(
		b,
		byte(s),
		byte(s>>8),
		byte(s>>16),
		byte(s>>24),
		byte(s>>32),
		byte(s>>40),
		byte(s>>48),
		byte(s>>56),
	)
}
