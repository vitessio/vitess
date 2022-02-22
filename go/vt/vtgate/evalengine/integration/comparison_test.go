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
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func perm(a []string, f func([]string)) {
	perm1(a, f, 0)
}

func perm1(a []string, f func([]string), i int) {
	if i > len(a) {
		f(a)
		return
	}
	perm1(a, f, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm1(a, f, i+1)
		a[i], a[j] = a[j], a[i]
	}
}

func normalize(v sqltypes.Value) string {
	typ := v.Type()
	if typ == sqltypes.Null {
		return "NULL"
	}
	if v.IsQuoted() || typ == sqltypes.Bit {
		return fmt.Sprintf("%v(%q)", typ, v.Raw())
	}
	if typ == sqltypes.Float32 || typ == sqltypes.Float64 {
		var bitsize = 64
		if typ == sqltypes.Float32 {
			bitsize = 32
		}
		f, err := strconv.ParseFloat(v.RawStr(), bitsize)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("%v(%s)", typ, evalengine.FormatFloat(typ, f))
	}
	return fmt.Sprintf("%v(%s)", typ, v.Raw())
}

var debugPrintAll = flag.Bool("print-all", false, "print all matching tests")
var debugNormalize = flag.Bool("normalize", true, "normalize comparisons against MySQL values")
var debugSimplify = flag.Bool("simplify", time.Now().UnixNano()&1 != 0, "simplify expressions before evaluating them")
var debugCheckTypes = flag.Bool("check-types", true, "check the TypeOf operator for all queries")

func compareRemoteQuery(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()

	local, localType, localErr := safeEvaluate(query)
	remote, remoteErr := conn.ExecuteFetch(query, 1, false)

	var localVal, remoteVal string
	if localErr == nil {
		v := local.Value()
		if *debugNormalize {
			localVal = normalize(v)
		} else {
			localVal = v.String()
		}
		if *debugCheckTypes {
			tt := v.Type()
			if tt != sqltypes.Null && tt != localType {
				t.Errorf("evaluation type mismatch: eval=%v vs typeof=%v\nlocal: %s\nquery: %s (SIMPLIFY=%v)",
					tt, localType, localVal, query, *debugSimplify)
			}
		}
	}
	if remoteErr == nil {
		if *debugNormalize {
			remoteVal = normalize(remote.Rows[0][0])
		} else {
			remoteVal = remote.Rows[0][0].String()
		}
	}
	if diff := compareResult(localErr, remoteErr, localVal, remoteVal); diff != "" {
		t.Errorf("%s\nquery: %s (SIMPLIFY=%v)", diff, query, *debugSimplify)
	} else if *debugPrintAll {
		t.Logf("local=%s mysql=%s\nquery: %s", localVal, remoteVal, query)
	}
}

func TestAllComparisons(t *testing.T) {
	var elems = []string{"NULL", "-1", "0", "1",
		`'foo'`, `'bar'`, `'FOO'`, `'BAR'`,
		`'foo' collate utf8mb4_as_cs`, // invalid collation for testing error messages
		`'foo' collate utf8mb4_cs_as`,
		`'FOO' collate utf8mb4_cs_as`,
		`_latin1 'foo'`,
		`_latin1 'FOO'`,
	}
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range operators {
		t.Run(op, func(t *testing.T) {
			for i := 0; i < len(elems); i++ {
				for j := 0; j < len(elems); j++ {
					query := fmt.Sprintf("SELECT %s %s %s", elems[i], op, elems[j])
					compareRemoteQuery(t, conn, query)
				}
			}
		})
	}
}

func TestAllTupleComparisons(t *testing.T) {
	var elems = []string{"NULL", "-1", "0", "1"}
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}

	var tuples []string
	perm(elems, func(t []string) {
		tuples = append(tuples, "("+strings.Join(t, ", ")+")")
	})

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range operators {
		t.Run(op, func(t *testing.T) {
			for i := 0; i < len(tuples); i++ {
				for j := 0; j < len(tuples); j++ {
					query := fmt.Sprintf("SELECT %s %s %s", tuples[i], op, tuples[j])
					compareRemoteQuery(t, conn, query)
				}
			}
		})
	}
}

func TestAllIsStatements(t *testing.T) {
	var left = []string{
		"NULL", "TRUE", "FALSE",
		`1`, `0`, `1.0`, `0.0`, `-1`, `666`,
		`"1"`, `"0"`, `"1.0"`, `"0.0"`, `"-1"`, `"666"`,
		`"POTATO"`, `""`, `" "`, `"    "`,
	}
	var right = []string{
		"NULL",
		"NOT NULL",
		"TRUE",
		"NOT TRUE",
		"FALSE",
		"NOT FALSE",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, l := range left {
		for _, r := range right {
			query := fmt.Sprintf("SELECT %s IS %s", l, r)
			compareRemoteQuery(t, conn, query)
		}
	}
}

func genSubsets1(args []string, subset []string, a, b int, yield func([]string)) {
	if a == len(subset) {
		yield(subset)
		return
	}
	if b >= len(args) {
		return
	}
	subset[a] = args[b]
	genSubsets1(args, subset, a+1, b+1, yield)
	genSubsets1(args, subset, a+0, b+1, yield)
}

func genSubsets(args []string, subsetLen int, yield func([]string)) {
	subset := make([]string, subsetLen)
	genSubsets1(args, subset, 0, 0, yield)
}

func TestMultiComparisons(t *testing.T) {
	var numbers = []string{
		`0`, `-1`, `1`, `0.0`, `1.0`, `-1.0`, `1.0E0`, `-1.0E0`, `0.0E0`,
		strconv.FormatUint(math.MaxUint64, 10),
		strconv.FormatUint(math.MaxInt64, 10),
		strconv.FormatInt(math.MinInt64, 10),
		`'foobar'`, `'FOOBAR'`,
		`"0"`, `"-1"`, `"1"`,
		`_utf8mb4 'foobar'`, `_utf8mb4 'FOOBAR'`,
		`_binary '0'`, `_binary '-1'`, `_binary '1'`,
		`0x0`, `0x1`, `-0x0`, `-0x1`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, method := range []string{"LEAST", "GREATEST"} {
		for _, argc := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s(#%d)", method, argc), func(t *testing.T) {
				genSubsets(numbers, argc, func(num []string) {
					query := fmt.Sprintf("SELECT %s(%s)", method, strings.Join(num, ","))
					compareRemoteQuery(t, conn, query)
				})
			})
		}
	}
}

func TestLikeComparison(t *testing.T) {
	var left = []string{
		`'foobar'`, `'FOOBAR'`,
		`'1234'`, `1234`,
		`_utf8mb4 'foobar' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOOBAR' COLLATE utf8mb4_0900_as_cs`,
	}
	var right = append([]string{
		`'foo%'`, `'FOO%'`, `'foo_ar'`, `'FOO_AR'`,
		`'12%'`, `'12_4'`,
		`_utf8mb4 'foo%' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOO%' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'foo_ar' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOO_AR' COLLATE utf8mb4_0900_as_cs`,
	}, left...)

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, lhs := range left {
		for _, rhs := range right {
			query := fmt.Sprintf("SELECT %s LIKE %s", lhs, rhs)
			compareRemoteQuery(t, conn, query)
		}
	}
}

func TestCollationOperations(t *testing.T) {
	var cases = []string{
		"COLLATION('foobar')",
		"COLLATION(_latin1 'foobar')",
		"COLLATION(_utf8mb4 'foobar' COLLATE utf8mb4_general_ci)",
		"COLLATION('foobar' COLLATE utf8mb4_general_ci)",
		"COLLATION(_latin1 'foobar' COLLATE utf8mb4_general_ci)",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, expr := range cases {
		compareRemoteQuery(t, conn, "SELECT "+expr)
	}
}

func TestNegateArithmetic(t *testing.T) {
	var cases = []string{
		`0`, `1`, `1.0`, `0.0`, `1.0e0`, `0.0e0`,
		`X'00'`, `X'1234'`, `X'ff'`,
		`0x00`, `0x1`, `0x1234`,
		`0xff`, `0xffff`, `0xffffffff`, `0xffffffffffffffff`,
		strconv.FormatUint(math.MaxUint64, 10),
		strconv.FormatUint(math.MaxInt64, 10),
		strconv.FormatInt(math.MinInt64, 10),
		`'foobar'`, `'FOOBAR'`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, rhs := range cases {
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT - %s", rhs))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%s", rhs))
	}
}

func TestNumericTypes(t *testing.T) {
	var numbers = []string{
		`1234`, `-1234`,
		`18446744073709551614`,
		`18446744073709551615`, // MaxUint64
		`18446744073709551616`,
		`-18446744073709551614`,
		`-18446744073709551615`, // -MaxUint64
		`-18446744073709551616`,
		`9223372036854775805`,
		`9223372036854775806`,
		`9223372036854775807`, // MaxInt64
		`9223372036854775808`, // -MinInt64
		`9223372036854775809`,
		`-9223372036854775805`,
		`-9223372036854775806`,
		`-9223372036854775807`, // -MaxInt64
		`-9223372036854775808`, // MinInt64
		`-9223372036854775809`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, rhs := range numbers {
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %s", rhs))
	}
}

func TestHexArithmetic(t *testing.T) {
	var cases = []string{
		`0`, `1`, `1.0`, `0.0`, `1.0e0`, `0.0e0`,
		`X'00'`, `X'1234'`, `X'ff'`,
		`0x00`, `0x1`, `0x1234`,
		`0xff`, `0xffff`, `0xffffffff`, `0xffffffffffffffff`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, lhs := range cases {
		for _, rhs := range cases {
			compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %s + %s", lhs, rhs))

			// compare with negative values too
			compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%s + -%s", lhs, rhs))
		}
	}
}

func TestTypes(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	var queries = []string{
		"1 > 3",
		"3 > 1",
		"-1 > -1",
		"1 = 1",
		"-1 = 1",
		"1 IN (1, -2, 3)",
		"1 LIKE 1",
		"-1 LIKE -1",
		"-1 LIKE 1",
		`"foo" IN ("bar", "FOO", "baz")`,
		`'pokemon' LIKE 'poke%'`,
		`(1, 2) = (1, 2)`,
		`1 = 'sad'`,
		`(1, 2) = (1, 3)`,
		`LEAST(0,0.0)`,
		`LEAST(0,1,0.0)`,
		`LEAST(0.0,0)`,
		`LEAST(0, 8446744073709551614)`,
		`"foobar"`,
		`X'444444'`,
		`_binary "foobar"`,
		`-0x0`,
	}

	for _, query := range queries {
		compareRemoteQuery(t, conn, "SELECT "+query)
	}
}

func TestFloatFormatting(t *testing.T) {
	var floats = []string{
		`18446744073709551615`,
		`9223372036854775807`,
		`0xff`, `0xffff`, `0xffffffff`,
		`0xffffffffffffffff`,
		`0xfffffffffffffffe`,
		`0xffffffffffffffff0`,
		`0x1fffffffffffffff`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, f := range floats {
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %s + 0.0e0", f))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%s", f))
	}

	for i := 0; i < 64; i++ {
		v := uint64(1) << i
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %d + 0.0e0", v))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %d + 0.0e0", v+1))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %d + 0.0e0", ^v))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%de0", v))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%de0", v+1))
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT -%de0", ^v))
	}
}

func TestWeightStrings(t *testing.T) {
	var inputs = []string{
		`'foobar'`, `_latin1 'foobar'`,
		`'foobar' as char(12)`, `'foobar' as binary(12)`,
		`_latin1 'foobar' as char(12)`, `_latin1 'foobar' as binary(12)`,
		`1234.0`, `12340e0`,
		`0x1234`, `0x1234 as char(12)`, `0x1234 as char(2)`,
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, i := range inputs {
		compareRemoteQuery(t, conn, fmt.Sprintf("SELECT WEIGHT_STRING(%s)", i))
	}
}

var bitwiseInputs = []string{
	"0", "1", "0xFF", "255", "1.0", "1.1", "-1", "-255", "7", "9", "13", "1.5", "-1.5",
	"0.0e0", "1.0e0", "255.0", "1.5e0", "-1.5e0", "1.1e0", "-1e0", "-255e0", "7e0", "9e0", "13e0",
	strconv.FormatUint(math.MaxUint64, 10),
	strconv.FormatUint(math.MaxInt64, 10),
	strconv.FormatInt(math.MinInt64, 10),
	`"foobar"`, `"foobar1234"`, `"0"`, "0x1", "-0x1", "X'ff'", "X'00'",
	`"1abcd"`, "NULL", `_binary "foobar"`, `_binary "foobar1234"`,
	"64", "'64'", "_binary '64'", "X'40'", "_binary X'40'",
}

func TestBitwiseOperators(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range []string{"&", "|", "^", "<<", ">>"} {
		t.Run(op, func(t *testing.T) {
			for _, lhs := range bitwiseInputs {
				for _, rhs := range bitwiseInputs {
					compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %s %s %s", lhs, op, rhs))
				}
			}
		})
	}
}

func TestBitwiseOperatorsUnary(t *testing.T) {
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, op := range []string{"~", "BIT_COUNT"} {
		t.Run(op, func(t *testing.T) {
			for _, rhs := range bitwiseInputs {
				compareRemoteQuery(t, conn, fmt.Sprintf("SELECT %s(%s)", op, rhs))
			}
		})
	}
}

func TestConversionOperators(t *testing.T) {
	var left = []string{
		"0", "1", "255",
		"0.0e0", "1.0e0", "1.5e0", "-1.5e0", "1.1e0", "-1.1e0", "-1.7e0",
		"0.0", "0.000", "1.5", "-1.5", "1.1", "1.7", "-1.1", "-1.7",
		`'foobar'`, `_utf8 'foobar'`, `''`, `_binary 'foobar'`,
		`0x0`, `0x1`, `0xff`, `X'00'`, `X'01'`, `X'ff'`,
		"NULL",
		"0xFF666F6F626172FF", "0x666F6F626172FF", "0xFF666F6F626172",
	}
	var right = []string{
		"BINARY", "BINARY(1)", "BINARY(0)", "BINARY(16)", "BINARY(-1)",
		"CHAR", "CHAR(1)", "CHAR(0)", "CHAR(16)", "CHAR(-1)",
		"NCHAR", "NCHAR(1)", "NCHAR(0)", "NCHAR(16)", "NCHAR(-1)",
		"DECIMAL", "DECIMAL(0, 4)", "DECIMAL(12, 0)", "DECIMAL(12, 4)",
		"DOUBLE", "REAL",
		"SIGNED", "UNSIGNED", "SIGNED INTEGER", "UNSIGNED INTEGER",
	}
	var conn = mysqlconn(t)
	defer conn.Close()

	for _, lhs := range left {
		for _, rhs := range right {
			compareRemoteQuery(t, conn, fmt.Sprintf("SELECT CAST(%s AS %s)", lhs, rhs))
			compareRemoteQuery(t, conn, fmt.Sprintf("SELECT CONVERT(%s, %s)", lhs, rhs))
		}
	}
}

func TestCharsetConversionOperators(t *testing.T) {
	var introducers = []string{
		"", "_latin1", "_utf8mb4", "_utf8", "_binary",
	}
	var contents = []string{
		`"foobar"`, `X'4D7953514C'`,
	}
	var charsets = []string{
		"utf8mb4", "utf8", "utf16", "utf32", "latin1", "ucs2",
	}

	var conn = mysqlconn(t)
	defer conn.Close()

	for _, pfx := range introducers {
		for _, lhs := range contents {
			for _, rhs := range charsets {
				compareRemoteQuery(t, conn, fmt.Sprintf("SELECT HEX(CONVERT(%s %s USING %s))", pfx, lhs, rhs))
			}
		}
	}
}
