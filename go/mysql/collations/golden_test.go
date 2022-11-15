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

package collations

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/testutil"
)

func TestGoldenWeights(t *testing.T) {
	gllGoldenTests, err := filepath.Glob("testdata/wiki_*.gob.gz")
	if err != nil {
		t.Fatal(err)
	}

	for _, goldenPath := range gllGoldenTests {
		golden := &testutil.GoldenTest{}
		if err := golden.DecodeFromFile(goldenPath); err != nil {
			t.Fatal(err)
		}

		for _, goldenCase := range golden.Cases {
			t.Run(fmt.Sprintf("%s (%s)", golden.Name, goldenCase.Lang), func(t *testing.T) {
				for coll, expected := range goldenCase.Weights {
					coll := testcollation(t, coll)

					input, err := charset.ConvertFromUTF8(nil, coll.Charset(), goldenCase.Text)
					if err != nil {
						t.Fatal(err)
					}

					result := coll.WeightString(nil, input, 0)
					assert.True(t, bytes.Equal(expected, result), "mismatch for collation=%s\noriginal: %s\ninput:    %#v\nexpected: %v\nactual:   %v", coll.Name(), string(goldenCase.Text), input, expected, result)

				}
			})
		}
	}
}

func TestCollationsForLanguage(t *testing.T) {
	allCollations := testall()
	langCounts := make(map[testutil.Lang][]string)

	for lang := range testutil.KnownLanguages {
		var matched []string
		for _, coll := range allCollations {
			name := coll.Name()
			if lang.MatchesCollation(name) {
				matched = append(matched, name)
			}
		}
		langCounts[lang] = matched
	}

	for lang := range testutil.KnownLanguages {
		assert.NotEqual(t, 0, len(langCounts[lang]), "no collations found for %q", lang)

		t.Logf("%s: %v", lang, langCounts[lang])
	}
}

// XTestSupportTables should not run by default; it is used to generate a Markdown
// table with Collation support information for the current build of Vitess.
func XTestSupportTables(t *testing.T) {
	var versions = []collver{
		collverMySQL80,
		collverMySQL57,
		collverMySQL56,
		collverMariaDB103,
		collverMariaDB102,
		collverMariaDB101,
		collverMariaDB100,
	}

	var envs []*Environment
	for _, v := range versions {
		envs = append(envs, makeEnv(v))
	}

	var all []ID
	for id := range globalVersionInfo {
		all = append(all, id)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i] < all[j]
	})

	var out = os.Stdout

	fmt.Fprintf(out, "| Collation | Charset")
	for _, env := range envs {
		fmt.Fprintf(out, " | %s", env.version.String())
	}
	fmt.Fprintf(out, " |\n|%s\n", strings.Repeat("---|", len(envs)+2))

	for _, id := range all {
		coll := globalAllCollations[id]
		if coll == nil {
			vdata := globalVersionInfo[id]

			var collnames []string
			for _, alias := range vdata.alias {
				collnames = append(collnames, alias.name)
				break
			}
			collname := strings.Join(collnames, ",")
			parts := strings.Split(collname, "_")

			fmt.Fprintf(out, "| %s | %s", collname, parts[0])
			for _, env := range envs {
				var supported bool
				for _, alias := range vdata.alias {
					if alias.mask&env.version != 0 {
						supported = true
						break
					}
				}
				if supported {
					fmt.Fprintf(out, " | ⚠️")
				} else {
					fmt.Fprintf(out, " | ❌")
				}
			}
		} else {
			fmt.Fprintf(out, "| %s | %s", coll.Name(), coll.Charset().Name())
			for _, env := range envs {
				_, supported := env.byID[coll.ID()]
				if supported {
					fmt.Fprintf(out, " | ✅")
				} else {
					fmt.Fprintf(out, " | ❌")
				}
			}
		}

		fmt.Fprintf(out, " |\n")
	}
}

func TestAllCollationsByCharset(t *testing.T) {
	var defaults1 = map[string][2]string{
		"utf8mb4": {"utf8mb4_general_ci", "utf8mb4_bin"},
	}
	var defaults2 = map[string][2]string{
		"utf8mb4": {"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin"},
	}

	for _, tc := range []struct {
		version  collver
		defaults map[string][2]string
	}{
		{collverMariaDB100, defaults1},
		{collverMariaDB101, defaults1},
		{collverMariaDB102, defaults1},
		{collverMariaDB103, defaults1},
		{collverMySQL56, defaults1},
		{collverMySQL57, defaults1},
		{collverMySQL80, defaults2},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			env := makeEnv(tc.version)
			for csname, cset := range env.byCharset {
				switch csname {
				case "gb18030":
					// this doesn't work yet
					continue
				}
				require.NotNil(t, cset.Default, "charset %s has no default", csname)
				require.NotNil(t, cset.Binary, "charset %s has no binary", csname)

			}

			for charset, expected := range tc.defaults {
				expectedDefault, expectedBinary := expected[0], expected[1]
				if def := env.DefaultCollationForCharset(charset); def.Name() != expectedDefault {
					t.Fatalf("bad default for utf8mb4: %s (expected %s)", def.Name(), expectedDefault)
				}
				if def := env.BinaryCollationForCharset(charset); def.Name() != expectedBinary {
					t.Fatalf("bad binary for utf8mb4: %s (expected %s)", def.Name(), expectedBinary)
				}
			}
		})
	}
}
