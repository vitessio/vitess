/*
Copyright 2021 The Vitess Authors.

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

package integration

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/remote"
)

func TestLocalEncodings(t *testing.T) {
	var cases = []struct {
		collation string
		input     []byte
	}{
		{
			collation: "latin1_swedish_ci",
			input:     []byte("abcdABCD01234"),
		},
	}

	conn := mysqlconn(t)
	defer conn.Close()

	for _, tc := range cases {
		local := collations.Local().LookupByName(tc.collation)
		remote := remote.NewCollation(conn, tc.collation)
		verifyTranscoding(t, local, remote, tc.input)
	}
}

func TestCJKStress(t *testing.T) {
	var universe [][]byte
	for cp := rune(0); cp <= 0x10FFFF; cp++ {
		if utf8.ValidRune(cp) {
			var b [16]byte
			l := utf8.EncodeRune(b[:], cp)

			block := int(cp / 256)
			for len(universe) <= block {
				universe = append(universe, nil)
			}
			universe[block] = append(universe[block], b[:l]...)
		}
	}

	var charsets = []charset.Charset{
		charset.Charset_latin1{},
		// charset.Charset_gb18030{},
		charset.Charset_gb2312{},
		charset.Charset_ujis{},
		charset.Charset_eucjpms{},
		charset.Charset_sjis{},
		charset.Charset_cp932{},
		charset.Charset_euckr{},
	}

	conn := mysqlconn(t)
	defer conn.Close()

	remoteUtf8mb4 := remote.NewCharset(conn, "utf8mb4")

	for _, local := range charsets {
		t.Run(local.Name(), func(t *testing.T) {
			remote := remote.NewCharset(conn, local.Name())
			convert := func(block []byte) ([]byte, []byte) {
				t.Helper()
				ours, _ := charset.ConvertFromUTF8(nil, local, block)
				theirs, err := charset.ConvertFromUTF8(nil, remote, block)
				require.NoError(t, err, "remote transcoding failed: %v", err)

				return ours, theirs
			}

			unconvert := func(block []byte) ([]byte, []byte) {
				t.Helper()
				ours, _ := charset.Convert(nil, charset.Charset_utf8mb4{}, block, local)
				theirs, err := charset.Convert(nil, remoteUtf8mb4, block, remote)
				require.NoError(t, err, "remote transcoding failed: %v", err)

				return ours, theirs
			}

			for _, block := range universe {
				if len(block) == 0 {
					continue
				}

				ours, theirs := convert(block)
				if !bytes.Equal(ours, theirs) {
					for _, cp := range string(block) {
						input := string(cp)
						ours, theirs := convert([]byte(input))
						require.True(t, bytes.Equal(ours, theirs), "%s: bad conversion for %q (U+%04X). ours: %#v, theirs: %#v", local.Name(), input, cp, ours, theirs)

					}
					panic("???")
				}

				ours2, theirs2 := unconvert(ours)
				if !bytes.Equal(ours2, theirs2) {
					for _, cp := range string(block) {
						input := string(cp)
						ours, _ := charset.ConvertFromUTF8(nil, local, []byte(input))

						ours2, theirs2 := unconvert(ours)
						require.True(t, bytes.Equal(ours2, theirs2), "%s: bad return conversion for %q (U+%04X) %#v. ours: %#v, theirs: %#v", local.Name(), input, cp, ours, ours2, theirs2)

					}
					panic("???")
				}
			}
		})
	}
}
