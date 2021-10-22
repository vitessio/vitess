package integration

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode/utf32"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/testutil"
	"vitess.io/vitess/go/mysql/collations/remote"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

func getSQLQueries(t *testing.T, testfile string) []string {
	tf, err := os.Open(testfile)
	if err != nil {
		t.Fatal(err)
	}
	defer tf.Close()

	var chunks []string
	var curchunk bytes.Buffer

	addchunk := func() {
		if curchunk.Len() > 0 {
			stmts, err := sqlparser.SplitStatementToPieces(curchunk.String())
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

var defaultUtf32 = utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM)

func parseUtf32cp(b []byte) []byte {
	var hexbuf [16]byte
	c, err := hex.Decode(hexbuf[:], b)
	if err != nil {
		return nil
	}
	utf8, _ := defaultUtf32.NewDecoder().Bytes(hexbuf[:c])
	return utf8
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
	coll := collations.LookupByName(u.collation)
	if coll == nil {
		t.Fatalf("unknown collation %q", u.collation)
	}

	var checked, errors int
	for _, row := range result.Rows {
		if row[1].Len() == 0 {
			continue
		}
		utf8Input := parseUtf32cp(row[0].ToBytes())
		if utf8Input == nil {
			t.Errorf("[%s] failed to parse UTF32-encoded codepoint: %s (%s)", u.collation, row[0], row[2].ToString())
			errors++
			continue
		}

		expectedWeightString := parseWeightString(row[1].ToBytes())
		if expectedWeightString == nil {
			t.Errorf("[%s] failed to parse weight string: %s (%s)", u.collation, row[1], row[2].ToString())
			errors++
			continue
		}

		weightString := coll.WeightString(make([]byte, 0, 128), utf8Input, 0)
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

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	res, err := conn.ExecuteFetch(query, -1, true)
	if err != nil {
		t.Fatalf("failed to execute %q: %v", query, err)
	}
	return res
}

func GoldenWeightString(t *testing.T, conn *mysql.Conn, collation string, input []byte) []byte {
	coll := remote.RemoteByName(conn, collation)
	weightString := coll.WeightString(nil, input, 0)
	if weightString == nil {
		t.Fatal(coll.LastError())
	}
	return weightString
}

const ExampleString = "abc æøå 日本語"

func TestCollationWithSpace(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	codepoints := len([]rune(ExampleString))

	for _, collName := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci"} {
		t.Run(collName, func(t *testing.T) {
			local := collations.LookupByName(collName)
			remote := remote.RemoteByName(conn, collName)

			for _, size := range []int{0, codepoints, codepoints + 1, codepoints + 2, 20, 32} {
				localWeight := local.WeightString(nil, []byte(ExampleString), size)
				remoteWeight := remote.WeightString(nil, []byte(ExampleString), size)
				if !bytes.Equal(localWeight, remoteWeight) {
					t.Fatalf("mismatch at len=%d\ninput:    %#v\nexpected: %#v\nactual:   %#v",
						size, []byte(ExampleString), remoteWeight, localWeight)
				}
			}
		})
	}
}

var testOneCollation = flag.String("test-one-collation", "", "")

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

type testweight struct {
	collation string
	input     []byte
}

type testcmp struct {
	collation   string
	left, right []byte
}

func testRemoteWeights(t *testing.T, golden io.Writer, cases []testweight) {
	conn := mysqlconn(t)
	defer conn.Close()

	for _, tc := range cases {
		t.Run(tc.collation, func(t *testing.T) {
			local := collations.LookupByName(tc.collation)
			remote := remote.RemoteByName(conn, tc.collation)
			localResult := local.WeightString(nil, tc.input, 0)
			remoteResult := remote.WeightString(nil, tc.input, 0)

			if err := remote.LastError(); err != nil {
				t.Fatalf("remote collation failed: %v", err)
			}

			if !bytes.Equal(localResult, remoteResult) {
				t.Errorf("expected WEIGHT_STRING(%#v) = %#v (got %#v)", tc.input, remoteResult, localResult)
			}

			if golden != nil {
				fmt.Fprintf(golden, "{\n\tcollation: %q,\n\texpected: %#v,\n},\n", tc.collation, remoteResult)
			}
		})
	}
}

func testRemoteComparison(t *testing.T, golden io.Writer, cases []testcmp) {
	normalizecmp := func(res int) int {
		if res < 0 {
			return -1
		}
		if res > 0 {
			return 1
		}
		return 0
	}

	conn := mysqlconn(t)
	defer conn.Close()

	for _, tc := range cases {
		t.Run(tc.collation, func(t *testing.T) {
			local := collations.LookupByName(tc.collation)
			remote := remote.RemoteByName(conn, tc.collation)
			localResult := normalizecmp(local.Collate(tc.left, tc.right, false))
			remoteResult := remote.Collate(tc.left, tc.right, false)

			if err := remote.LastError(); err != nil {
				t.Fatalf("remote collation failed: %v", err)
			}
			if localResult != remoteResult {
				t.Errorf("expected STRCMP(%q, %q) = %d (got %d)", string(tc.left), string(tc.right), remoteResult, localResult)
			}
			if golden != nil {
				fmt.Fprintf(golden, "{\n\tcollation: %q,\n\tleft: %#v,\n\tright: %#v,\n\texpected: %d,\n},\n",
					tc.collation, tc.left, tc.right, remoteResult)
			}
		})
	}
}

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

func TestRemoteKanaSensitivity(t *testing.T) {
	var Kana1 = []byte("の東京ノ")
	var Kana2 = []byte("ノ東京の")

	testRemoteComparison(t, nil, []testcmp{
		{"utf8mb4_0900_as_cs", Kana1, Kana2},
		{"utf8mb4_ja_0900_as_cs", Kana1, Kana2},
		{"utf8mb4_ja_0900_as_cs_ks", Kana1, Kana2},
	})
}

var flagDumpBadCases = flag.Bool("dump-bad-cases", false, "dump strings that fail a test to a tmpfile")

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

	allCollations := collations.All()
	sort.Slice(allCollations, func(i, j int) bool {
		return allCollations[i].Id() < allCollations[j].Id()
	})
	for _, coll := range allCollations {
		cs := coll.Charset()
		c4cs := charsetMap[cs.Name()]
		if c4cs == nil {
			c4cs = &collationsForCharset{charset: cs}
			charsetMap[cs.Name()] = c4cs
		}

		c4cs.locals = append(c4cs.locals, coll)
		c4cs.remotes = append(c4cs.remotes, remote.RemoteByName(conn, coll.Name()))
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
			text := goldencase.Text //[]byte(string([]rune(string(goldencase.Text))[:64]))

			transLocal, err := c4cs.charset.EncodeFromUTF8(text)
			if err != nil {
				continue
			}

			transRemote, err := c4cs.remotes[0].Charset().EncodeFromUTF8(text)
			if err != nil {
				t.Fatalf("remote transcoding failed: %v", err)
			}

			if !bytes.Equal(transLocal, transRemote) {
				t.Fatalf("transcoding mismatch for %q with charset %s\ninput:\n%s\nremote:\n%s\nlocal:\n%s\n",
					goldencase.Lang, c4cs.charset.Name(), hex.Dump(text), hex.Dump(transRemote), hex.Dump(transLocal))
			}

			for i := range c4cs.locals {
				local := c4cs.locals[i]
				remote := c4cs.remotes[i]

				localResult := local.WeightString(nil, transLocal, 0)
				remoteResult := remote.WeightString(nil, transLocal, 0)

				if err := remote.LastError(); err != nil {
					t.Fatalf("remote collation failed: %v", err)
				}

				if len(remoteResult) == 0 {
					t.Logf("remote collation %s returned empty string", remote.Name())
					continue
				}

				if !bytes.Equal(localResult, remoteResult) {
					var colldumpDebug string
					if *flagDumpBadCases {
						bad, err := os.CreateTemp("", "vitess_collation_example")
						if err != nil {
							t.Fatal(err)
						}
						bad.Write(transLocal)
						bad.Close()

						colldumpDebug = fmt.Sprintf("manual debugging:\n\tcolldump --test %s < %s\n\n", local.Name(), bad.Name())
					}
					t.Fatalf("WEIGHT_STRING mismatch for %q with collation %s (charset %s)\ninput:\n%s\nremote:\n%s\nlocal:\n%s\ngolden:\n%#v\n\n%s",
						goldencase.Lang, local.Name(), c4cs.charset.Name(), hex.Dump(transLocal), hex.Dump(remoteResult), hex.Dump(localResult), transLocal, colldumpDebug)
				}
			}

			tested++
		}
		t.Logf("%q: %d collations, %d test strings = %d tests",
			c4cs.charset.Name(), len(c4cs.locals), tested, len(c4cs.locals)*tested)
	}
}
