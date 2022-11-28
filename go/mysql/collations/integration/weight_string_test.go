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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/testutil"
	"vitess.io/vitess/go/mysql/collations/remote"
)

func TestFastIterators(t *testing.T) {
	input := make([]byte, 128)
	for n := range input {
		input[n] = byte(n)
	}
	input[0] = 'A'

	testRemoteWeights(t, nil, []testweight{
		{"utf8mb4_0900_as_cs", input},
		{"utf8mb4_0900_as_ci", input},
		{"utf8mb4_0900_ai_ci", input},
	})
}

func TestWeightStringsComprehensive(t *testing.T) {
	type collationsForCharset struct {
		charset charset.Charset
		locals  []collations.Collation
		remotes []*remote.Collation
	}
	var charsetMap = make(map[string]*collationsForCharset)

	golden := &testutil.GoldenTest{}
	if err := golden.DecodeFromFile("../testdata/wiki_416c626572742045696e737465696e.gob.gz"); err != nil {
		t.Fatal(err)
	}

	conn := mysqlconn(t)
	defer conn.Close()

	allCollations := collations.Local().AllCollations()
	sort.Slice(allCollations, func(i, j int) bool {
		return allCollations[i].ID() < allCollations[j].ID()
	})
	for _, coll := range allCollations {
		cs := coll.Charset()
		c4cs := charsetMap[cs.Name()]
		if c4cs == nil {
			c4cs = &collationsForCharset{charset: cs}
			charsetMap[cs.Name()] = c4cs
		}

		c4cs.locals = append(c4cs.locals, coll)
		c4cs.remotes = append(c4cs.remotes, remote.NewCollation(conn, coll.Name()))
	}

	var allCharsets []*collationsForCharset
	for _, c4cs := range charsetMap {
		allCharsets = append(allCharsets, c4cs)
	}
	sort.Slice(allCharsets, func(i, j int) bool {
		return allCharsets[i].charset.Name() < allCharsets[j].charset.Name()
	})

	for _, c4cs := range allCharsets {
		var tested int
		for _, goldencase := range golden.Cases {
			text := []byte(string([]rune(string(goldencase.Text))[:64]))
			if trans := verifyTranscoding(t, c4cs.locals[0], c4cs.remotes[0], text); trans != nil {
				for i := range c4cs.locals {
					verifyWeightString(t, c4cs.locals[i], c4cs.remotes[i], trans)
					tested++
				}
			}
		}
		t.Logf("%q: %d collations, %d test strings = %d tests",
			c4cs.charset.Name(), len(c4cs.locals), tested, len(c4cs.locals)*tested)
	}
}

func TestCJKWeightStrings(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	allCollations := collations.Local().AllCollations()
	testdata, _ := filepath.Glob("../internal/charset/testdata/*.txt")
	for _, testfile := range testdata {
		charset := filepath.Base(testfile)
		charset = strings.TrimSuffix(charset, ".txt")
		charset = charset[strings.LastIndexByte(charset, '-')+1:]

		var valid []collations.Collation
		for _, coll := range allCollations {
			if coll.Charset().Name() == charset {
				valid = append(valid, coll)
				t.Logf("%s -> %s", testfile, coll.Name())
			}
		}
		if len(valid) == 0 {
			continue
		}
		text, err := os.ReadFile(testfile)
		if err != nil {
			t.Fatal(err)
		}
		for _, coll := range valid {
			verifyWeightString(t, coll, remote.NewCollation(conn, coll.Name()), text)
		}
	}
}
