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
	"encoding/hex"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/remote"
)

func TestRemoteWildcardMatches(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	var cases = []struct {
		in, pat string
	}{
		{"abc", "abc"},
		{"Abc", "aBc"},
		{"abc", "_bc"},
		{"abc", "a_c"},
		{"abc", "ab_"},
		{"abc", "%c"},
		{"abc", "a%c"},
		{"abc", "a%"},
		{"abcdef", "a%d_f"},
		{"abcdefg", "a%d%g"},
		{"a\\", "a\\"},
		{"aa\\", "a%\\"},
		{"Y", "\u00dd"},
		{"abcd", "abcde"},
		{"abcde", "abcd"},
		{"abcde", "a%f"},
		{"abcdef", "a%%f"},
		{"abcd", "a__d"},
		{"abcd", "a\\bcd"},
		{"a\\bcd", "abcd"},
		{"abdbcd", "a%cd"},
		{"abecd", "a%bd"},
		{"ǎḄÇ", "Ǎḅç"},
		{"ÁḆĈ", "Ǎḅç"},
		{"ǍBc", "_bc"},
		{"Aḅc", "a_c"},
		{"Abç", "ab_"},
		{"Ǎḅç", "%ç"},
		{"Ǎḅç", "ǎ%Ç"},
		{"aḅç", "a%"},
		{"Ǎḅçdef", "ǎ%d_f"},
		{"Ǎḅçdefg", "ǎ%d%g"},
		{"ǎ\\", "Ǎ\\"},
		{"ǎa\\", "Ǎ%\\"},
		{"Y", "\u00dd"},
		{"abcd", "Ǎḅçde"},
		{"abcde", "Ǎḅçd"},
		{"Ǎḅçde", "a%f"},
		{"Ǎḅçdef", "ǎ%%f"},
		{"Ǎḅçd", "ǎ__d"},
		{"Ǎḅçd", "ǎ\\ḄÇd"},
		{"a\\bcd", "Ǎḅçd"},
		{"Ǎḅdbçd", "ǎ%Çd"},
		{"Ǎḅeçd", "a%bd"},
	}

	for _, local := range collations.Local().AllCollations() {
		t.Run(local.Name(), func(t *testing.T) {
			var remote = remote.NewCollation(conn, local.Name())
			var err error
			var chEscape = '\\'

			if !charset.IsBackslashSafe(local.Charset()) {
				chEscape = '/'
			}

			for _, tc := range cases {
				input, pat := []byte(tc.in), []byte(tc.pat)

				if chEscape != '\\' {
					for i := range pat {
						if pat[i] == '\\' {
							pat[i] = byte(chEscape)
						}
					}
				}

				input, err = charset.ConvertFromUTF8(nil, local.Charset(), input)
				if err != nil {
					continue
				}
				pat, err = charset.ConvertFromUTF8(nil, local.Charset(), pat)
				if err != nil {
					continue
				}

				localResult := local.Wildcard(pat, 0, 0, chEscape).Match(input)
				remoteResult := remote.Wildcard(pat, 0, 0, chEscape).Match(input)
				if err := remote.LastError(); err != nil {
					t.Fatalf("remote collation failed: %v", err)
				}
				if localResult != remoteResult {
					t.Errorf("expected %q LIKE %q = %v (got %v)", tc.in, tc.pat, remoteResult, localResult)

					printDebugData(t, []string{
						"wildcmp",
						"--collation", local.Name(),
						"--input", hex.EncodeToString(input),
						"--pattern", hex.EncodeToString(pat),
					})
				}
			}
		})
	}
}
