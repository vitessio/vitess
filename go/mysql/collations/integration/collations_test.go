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
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/colldata"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/remote"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

var collationEnv *collations.Environment

func init() {
	// We require MySQL 8.0 collations for the comparisons in the tests
	collationEnv = collations.NewEnvironment("8.0.30")
}

func getSQLQueries(t *testing.T, testfile string) []string {
	tf, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer tf.Close()

	var chunks []string
	var curchunk strings.Builder

	addchunk := func() {
		if curchunk.Len() > 0 {
			stmts, err := sqlparser.NewTestParser().SplitStatementToPieces(curchunk.String())
			if err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, stmts...)
			curchunk.Reset()
		}
	}

	scanner := bufio.NewScanner(tf)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "--") {
			addchunk()
			chunks = append(chunks, scanner.Text())
		} else {
			if curchunk.Len() > 0 {
				curchunk.WriteByte(' ')
			}
			curchunk.Write(scanner.Bytes())
		}
	}
	addchunk()
	return chunks
}

type TestOnResults interface {
	Test(t *testing.T, result *sqltypes.Result)
}

type uca900CollationTest struct {
	collation string
}

func decodeUtf32(dst, src []byte) ([]byte, error) {
	for len(src) >= 4 {
		r := rune(uint32(src[0])<<24 | uint32(src[1])<<16 | uint32(src[2])<<8 | uint32(src[3]))
		dst = utf8.AppendRune(dst, r)
		src = src[4:]
	}
	if len(src) != 0 {
		return nil, errors.New("short src")
	}
	return dst, nil
}

func parseUtf32cp(b []byte) []byte {
	var hexbuf [16]byte
	c, err := hex.Decode(hexbuf[:], b)
	if err != nil {
		return nil
	}
	dst, err := decodeUtf32(nil, hexbuf[:c])
	if err != nil {
		panic("failed to decode utf32")
	}
	return dst
}

func parseWeightString(b []byte) []byte {
	dst := make([]byte, hex.DecodedLen(len(b)))
	n, err := hex.Decode(dst, b)
	if err != nil {
		return nil
	}
	return dst[:n]
}

func (u *uca900CollationTest) Test(t *testing.T, result *sqltypes.Result) {
	coll := collationEnv.LookupByName(u.collation)
	require.NotNil(t, coll, "unknown collation %q", u.collation)

	var checked, errors int
	for _, row := range result.Rows {
		if row[1].Len() == 0 {
			continue
		}
		rowBytes, err := row[0].ToBytes()
		require.NoError(t, err)
		utf8Input := parseUtf32cp(rowBytes)
		if utf8Input == nil {
			t.Errorf("[%s] failed to parse UTF32-encoded codepoint: %s (%s)", u.collation, row[0], row[2].ToString())
			errors++
			continue
		}
		rowBytes, err = row[1].ToBytes()
		require.NoError(t, err)
		expectedWeightString := parseWeightString(rowBytes)
		if expectedWeightString == nil {
			t.Errorf("[%s] failed to parse weight string: %s (%s)", u.collation, row[1], row[2].ToString())
			errors++
			continue
		}

		weightString := colldata.Lookup(coll).WeightString(make([]byte, 0, 128), utf8Input, 0)
		if !bytes.Equal(weightString, expectedWeightString) {
			t.Errorf("[%s] mismatch for %s (%v): \n\twant: %v\n\tgot:  %v", u.collation, row[2].ToString(), utf8Input, expectedWeightString, weightString)
			errors++
		}
		checked++
	}

	t.Logf("uca900CollationTest[%s]: checked %d codepoints, %d failed (%.02f%%)", u.collation, checked, errors, float64(errors)/float64(checked)*100.0)
}

func processSQLTest(t *testing.T, testfile string, conn *mysql.Conn) {
	var curtest TestOnResults

	for _, query := range getSQLQueries(t, testfile) {
		if strings.HasPrefix(query, "--") {
			switch {
			case strings.HasPrefix(query, "--source "):
				include := strings.TrimPrefix(query, "--source ")
				include = path.Join("testdata/mysqltest", include)
				processSQLTest(t, include, conn)

			case strings.HasPrefix(query, "--test:uca0900 "):
				collation := strings.TrimPrefix(query, "--test:uca0900 ")
				curtest = &uca900CollationTest{collation}

			case query == "--disable_warnings" || query == "--enable_warnings":
			case query == "--disable_query_log" || query == "--enable_query_log":

			default:
				t.Logf("unsupported statement: %q", query)
			}
			continue
		}

		res := exec(t, conn, query)
		if curtest != nil {
			curtest.Test(t, res)
			curtest = nil
		}
	}
}

var testOneCollation = pflag.String("test-one-collation", "", "")

func TestCollationsOnMysqld(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	if *testOneCollation != "" {
		processSQLTest(t, fmt.Sprintf("testdata/mysqltest/suite/collations/%s.test", *testOneCollation), conn)
		return
	}

	testfiles, _ := filepath.Glob("testdata/mysqltest/suite/collations/*.test")
	for _, testfile := range testfiles {
		t.Run(testfile, func(t *testing.T) {
			processSQLTest(t, testfile, conn)
		})
	}
}

func TestRemoteKanaSensitivity(t *testing.T) {
	Kana1 := []byte("の東京ノ")
	Kana2 := []byte("ノ東京の")

	testRemoteComparison(t, nil, []testcmp{
		{"utf8mb4_0900_as_cs", Kana1, Kana2},
		{"utf8mb4_ja_0900_as_cs", Kana1, Kana2},
		{"utf8mb4_ja_0900_as_cs_ks", Kana1, Kana2},
	})
}

const ExampleString = "abc æøå 日本語"

func TestCollationWithSpace(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	codepoints := len([]rune(ExampleString))

	for _, collName := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci"} {
		t.Run(collName, func(t *testing.T) {
			local := collationEnv.LookupByName(collName)
			remote := remote.NewCollation(conn, collName)

			for _, size := range []int{0, codepoints, codepoints + 1, codepoints + 2, 20, 32} {
				localWeight := colldata.Lookup(local).WeightString(nil, []byte(ExampleString), size)
				remoteWeight := remote.WeightString(nil, []byte(ExampleString), size)
				require.True(t, bytes.Equal(localWeight, remoteWeight), "mismatch at len=%d\ninput:    %#v\nexpected: %#v\nactual:   %#v", size, []byte(ExampleString), remoteWeight, localWeight)

			}
		})
	}
}
