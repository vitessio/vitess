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

package colldata

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/testutil"
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
