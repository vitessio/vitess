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
	"path/filepath"
	"testing"

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
					if !bytes.Equal(expected, result) {
						t.Errorf("mismatch for collation=%s\noriginal: %s\ninput:    %#v\nexpected: %v\nactual:   %v",
							coll.Name(), string(goldenCase.Text), input, expected, result)
					}
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
		if len(langCounts[lang]) == 0 {
			t.Errorf("no collations found for %q", lang)
		}
		t.Logf("%s: %v", lang, langCounts[lang])
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
				if cset.Default == nil {
					t.Fatalf("charset %s has no default", csname)
				}
				if cset.Binary == nil {
					t.Fatalf("charset %s has no binary", csname)
				}
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
