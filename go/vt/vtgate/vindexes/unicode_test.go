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
	"crypto/md5"
	"hash"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestNormalization(t *testing.T) {
	tcases := []struct {
		in, out string
	}{{
		in:  "Test",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "TEST",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Te\u0301st",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Tést",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Bést",
		out: "\x16\x05\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Test ",
		out: "\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  " Test",
		out: "\x01\t\x18\x16\x16L\x17\xf3\x18\x16",
	}, {
		in:  "Test\t",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x01\x00",
	}, {
		in:  "TéstLooong",
		out: "\x18\x16\x16L\x17\xf3\x18\x16\x17\x11\x17q\x17q\x17q\x17O\x16\x91",
	}, {
		in:  "T",
		out: "\x18\x16",
	}}

	hashes := []struct {
		name     string
		collator *sync.Pool
		hasher   func() hash.Hash
	}{
		{"XXHash", &collateXX, func() hash.Hash { return XXHashBigEndian{xxhash.New()} }},
		{"MD5", &collateMD5, md5.New},
	}

	for _, impl := range hashes {
		t.Run(impl.name, func(t *testing.T) {
			for _, tc := range tcases {
				h := impl.hasher()
				h.Write([]byte(tc.out))
				expected := h.Sum(nil)

				got, err := unicodeHash(impl.collator, sqltypes.NewVarChar(tc.in))
				require.NoError(t, err)

				assert.Equal(t, expected, got)
			}
		})
	}
}
