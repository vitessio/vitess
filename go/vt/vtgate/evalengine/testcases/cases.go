/*
Copyright 2023 The Vitess Authors.

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

package testcases

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type TestCase interface {
	Test(yield Iterator)
	Environment() *evalengine.ExpressionEnv
}

type Iterator func(query string, row []sqltypes.Value)

type defaultEnv struct{}

func (defaultEnv) Environment() *evalengine.ExpressionEnv {
	return evalengine.EnvWithBindVars(nil, collations.CollationUtf8mb4ID)
}

type JSONPathOperations struct{ defaultEnv }
type JSONArray struct{ defaultEnv }
type JSONObject struct{ defaultEnv }
type CharsetConversionOperators struct{ defaultEnv }
type CaseExprWithPredicate struct{ defaultEnv }
type Ceil struct{ defaultEnv }
type CaseExprWithValue struct{ defaultEnv }
type Base64 struct{ defaultEnv }
type Conversion struct{ defaultEnv }
type LargeDecimals struct{ defaultEnv }
type LargeIntegers struct{ defaultEnv }
type DecimalClamping struct{ defaultEnv }
type BitwiseOperatorsUnary struct{ defaultEnv }
type BitwiseOperators struct{ defaultEnv }
type WeightString struct{ defaultEnv }
type FloatFormatting struct{ defaultEnv }
type UnderscoreAndPercentage struct{ defaultEnv }
type Types struct{ defaultEnv }
type HexArithmetic struct{ defaultEnv }
type NumericTypes struct{ defaultEnv }
type NegateArithmetic struct{ defaultEnv }
type CollationOperations struct{ defaultEnv }
type LikeComparison struct{ defaultEnv }
type MultiComparisons struct{ defaultEnv }
type IsStatement struct{ defaultEnv }
type TupleComparisons struct{ defaultEnv }
type Comparisons struct{ defaultEnv }
type JSONExtract struct{}
type FnLower struct{ defaultEnv }
type FnUpper struct{ defaultEnv }
type FnCharLength struct{ defaultEnv }
type FnLength struct{ defaultEnv }
type FnBitLength struct{ defaultEnv }
type FnAscii struct{ defaultEnv }
type FnRepeat struct{ defaultEnv }
type FnHex struct{ defaultEnv }

var Cases = []TestCase{
	JSONExtract{},
	JSONPathOperations{},
	JSONArray{},
	JSONObject{},
	CharsetConversionOperators{},
	CaseExprWithPredicate{},
	Ceil{},
	CaseExprWithValue{},
	Base64{},
	Conversion{},
	LargeDecimals{},
	LargeIntegers{},
	DecimalClamping{},
	BitwiseOperatorsUnary{},
	BitwiseOperators{},
	WeightString{},
	FloatFormatting{},
	UnderscoreAndPercentage{},
	Types{},
	HexArithmetic{},
	NumericTypes{},
	NegateArithmetic{},
	CollationOperations{},
	LikeComparison{},
	MultiComparisons{},
	IsStatement{},
	TupleComparisons{},
	Comparisons{},
	FnLower{},
	FnUpper{},
	FnCharLength{},
	FnLength{},
	FnBitLength{},
	FnAscii{},
	FnRepeat{},
	FnHex{},
}

func (JSONPathOperations) Test(yield Iterator) {
	for _, obj := range inputJSONObjects {
		yield(fmt.Sprintf("JSON_KEYS('%s')", obj), nil)

		for _, path1 := range inputJSONPaths {
			yield(fmt.Sprintf("JSON_EXTRACT('%s', '%s')", obj, path1), nil)
			yield(fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'one', '%s')", obj, path1), nil)
			yield(fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'all', '%s')", obj, path1), nil)
			yield(fmt.Sprintf("JSON_KEYS('%s', '%s')", obj, path1), nil)

			for _, path2 := range inputJSONPaths {
				yield(fmt.Sprintf("JSON_EXTRACT('%s', '%s', '%s')", obj, path1, path2), nil)
				yield(fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'one', '%s', '%s')", obj, path1, path2), nil)
				yield(fmt.Sprintf("JSON_CONTAINS_PATH('%s', 'all', '%s', '%s')", obj, path1, path2), nil)
			}
		}
	}
}

func (JSONArray) Test(yield Iterator) {
	for _, a := range inputJSONPrimitives {
		yield(fmt.Sprintf("JSON_ARRAY(%s)", a), nil)
		for _, b := range inputJSONPrimitives {
			yield(fmt.Sprintf("JSON_ARRAY(%s, %s)", a, b), nil)
		}
	}
	yield("JSON_ARRAY()", nil)
}

func (JSONObject) Test(yield Iterator) {
	for _, a := range inputJSONPrimitives {
		for _, b := range inputJSONPrimitives {
			yield(fmt.Sprintf("JSON_OBJECT(%s, %s)", a, b), nil)
		}
	}
	yield("JSON_OBJECT()", nil)
}

func (CharsetConversionOperators) Test(yield Iterator) {
	var introducers = []string{
		"", "_latin1", "_utf8mb4", "_utf8", "_binary",
	}
	var contents = []string{
		`"foobar"`, `X'4D7953514C'`,
	}
	var charsets = []string{
		"utf8mb4", "utf8", "utf16", "utf32", "latin1", "ucs2",
	}

	for _, pfx := range introducers {
		for _, lhs := range contents {
			for _, rhs := range charsets {
				yield(fmt.Sprintf("HEX(CONVERT(%s %s USING %s))", pfx, lhs, rhs), nil)
			}
		}
	}
}

func (CaseExprWithPredicate) Test(yield Iterator) {
	var predicates = []string{
		"true",
		"false",
		"null",
		"1=1",
		"1=2",
	}

	var elements []string
	elements = append(elements, inputBitwise...)
	elements = append(elements, inputComparisonElement...)

	for _, pred1 := range predicates {
		for _, val1 := range elements {
			for _, elseVal := range elements {
				yield(fmt.Sprintf("case when %s then %s else %s end", pred1, val1, elseVal), nil)
			}
		}
	}

	genSubsets(predicates, 3, func(predicates []string) {
		genSubsets(elements, 3, func(values []string) {
			yield(fmt.Sprintf("case when %s then %s when %s then %s when %s then %s end",
				predicates[0], values[0], predicates[1], values[1], predicates[2], values[2],
			), nil)
		})
	})
}

func (Ceil) Test(yield Iterator) {
	var ceilInputs = []string{
		"0",
		"1",
		"-1",
		"'1.5'",
		"NULL",
		"'ABC'",
		"1.5e0",
		"-1.5e0",
		"9223372036854775810.4",
		"-9223372036854775810.4",
	}

	for _, num := range ceilInputs {
		yield(fmt.Sprintf("CEIL(%s)", num), nil)
		yield(fmt.Sprintf("CEILING(%s)", num), nil)
	}
}

// HACK: for CASE comparisons, the expression is supposed to decompose like this:
//
//	CASE a WHEN b THEN bb WHEN c THEN cc ELSE d
//		=> CASE WHEN a = b THEN bb WHEN a == c THEN cc ELSE d
//
// See: https://dev.mysql.com/doc/refman/5.7/en/flow-control-functions.html#operator_case
// However, MySQL does not seem to be using the real `=` operator for some of these comparisons
// namely, numerical comparisons are coerced into an unsigned form when they shouldn't.
// Example:
//
//	SELECT -1 = 18446744073709551615
//		=> 0
//	SELECT -1 WHEN 18446744073709551615 THEN 1 ELSE 0 END
//		=> 1
//
// This does not happen for other types, which all follow the behavior of the `=` operator,
// so we're going to assume this is a bug for now.
func comparisonSkip(a, b string) bool {
	if a == "-1" && b == "18446744073709551615" {
		return true
	}
	if b == "-1" && a == "18446744073709551615" {
		return true
	}
	if a == "9223372036854775808" && b == "-9223372036854775808" {
		return true
	}
	if a == "-9223372036854775808" && b == "9223372036854775808" {
		return true
	}
	return false
}

func (CaseExprWithValue) Test(yield Iterator) {
	var elements []string
	elements = append(elements, inputBitwise...)
	elements = append(elements, inputComparisonElement...)

	for _, cmpbase := range elements {
		for _, val1 := range elements {
			if comparisonSkip(cmpbase, val1) {
				continue
			}
			yield(fmt.Sprintf("case %s when %s then 1 else 0 end", cmpbase, val1), nil)
		}
	}
}

func (Base64) Test(yield Iterator) {
	var inputs = []string{
		`'bGlnaHQgdw=='`,
		`'bGlnaHQgd28='`,
		`'bGlnaHQgd29y'`,
	}

	inputs = append(inputs, inputConversions...)
	for _, input := range inputConversions {
		inputs = append(inputs, "'"+base64.StdEncoding.EncodeToString([]byte(input))+"'")
	}

	for _, lhs := range inputs {
		yield(fmt.Sprintf("FROM_BASE64(%s)", lhs), nil)
		yield(fmt.Sprintf("TO_BASE64(%s)", lhs), nil)
	}
}

func (Conversion) Test(yield Iterator) {
	var right = []string{
		"BINARY", "BINARY(1)", "BINARY(0)", "BINARY(16)", "BINARY(-1)",
		"CHAR", "CHAR(1)", "CHAR(0)", "CHAR(16)", "CHAR(-1)",
		"NCHAR", "NCHAR(1)", "NCHAR(0)", "NCHAR(16)", "NCHAR(-1)",
		"DECIMAL", "DECIMAL(0, 4)", "DECIMAL(12, 0)", "DECIMAL(12, 4)",
		"DOUBLE", "REAL",
		"SIGNED", "UNSIGNED", "SIGNED INTEGER", "UNSIGNED INTEGER", "JSON",
	}
	for _, lhs := range inputConversions {
		for _, rhs := range right {
			yield(fmt.Sprintf("CAST(%s AS %s)", lhs, rhs), nil)
			yield(fmt.Sprintf("CONVERT(%s, %s)", lhs, rhs), nil)
			yield(fmt.Sprintf("CAST(CAST(%s AS JSON) AS %s)", lhs, rhs), nil)
		}
	}
}

func (LargeDecimals) Test(yield Iterator) {
	var largepi = inputPi + inputPi

	for pos := 0; pos < len(largepi); pos++ {
		yield(fmt.Sprintf("%s.%s", largepi[:pos], largepi[pos:]), nil)
		yield(fmt.Sprintf("-%s.%s", largepi[:pos], largepi[pos:]), nil)
	}
}

func (LargeIntegers) Test(yield Iterator) {
	var largepi = inputPi + inputPi

	for pos := 1; pos < len(largepi); pos++ {
		yield(largepi[:pos], nil)
		yield(fmt.Sprintf("-%s", largepi[:pos]), nil)
	}
}

func (DecimalClamping) Test(yield Iterator) {
	for pos := 0; pos < len(inputPi); pos++ {
		for m := 0; m < min(len(inputPi), 67); m += 2 {
			for d := 0; d <= min(m, 33); d += 2 {
				yield(fmt.Sprintf("CAST(%s.%s AS DECIMAL(%d, %d))", inputPi[:pos], inputPi[pos:], m, d), nil)
			}
		}
	}
}

func (BitwiseOperatorsUnary) Test(yield Iterator) {
	for _, op := range []string{"~", "BIT_COUNT"} {
		for _, rhs := range inputBitwise {
			yield(fmt.Sprintf("%s(%s)", op, rhs), nil)
		}
	}
}

func (BitwiseOperators) Test(yield Iterator) {
	for _, op := range []string{"&", "|", "^", "<<", ">>"} {
		for _, lhs := range inputBitwise {
			for _, rhs := range inputBitwise {
				yield(fmt.Sprintf("%s %s %s", lhs, op, rhs), nil)
			}
		}
	}
}

func (WeightString) Test(yield Iterator) {
	var inputs = []string{
		`'foobar'`, `_latin1 'foobar'`,
		`'foobar' as char(12)`, `'foobar' as binary(12)`,
		`_latin1 'foobar' as char(12)`, `_latin1 'foobar' as binary(12)`,
		`1234.0`, `12340e0`,
		`0x1234`, `0x1234 as char(12)`, `0x1234 as char(2)`,
	}

	for _, i := range inputs {
		yield(fmt.Sprintf("WEIGHT_STRING(%s)", i), nil)
	}
}

func (FloatFormatting) Test(yield Iterator) {
	var floats = []string{
		`18446744073709551615`,
		`9223372036854775807`,
		`0xff`, `0xffff`, `0xffffffff`,
		`0xffffffffffffffff`,
		`0xfffffffffffffffe`,
		`0xffffffffffffffff0`,
		`0x1fffffffffffffff`,
		"18446744073709540000e0",
	}

	for _, f := range floats {
		yield(fmt.Sprintf("%s + 0.0e0", f), nil)
		yield(fmt.Sprintf("-%s", f), nil)
	}

	for i := 0; i < 64; i++ {
		v := uint64(1) << i
		yield(fmt.Sprintf("%d + 0.0e0", v), nil)
		yield(fmt.Sprintf("%d + 0.0e0", v+1), nil)
		yield(fmt.Sprintf("%d + 0.0e0", ^v), nil)
		yield(fmt.Sprintf("-%de0", v), nil)
		yield(fmt.Sprintf("-%de0", v+1), nil)
		yield(fmt.Sprintf("-%de0", ^v), nil)
	}
}

func (UnderscoreAndPercentage) Test(yield Iterator) {
	var queries = []string{
		`'pokemon' LIKE 'poke%'`,
		`'pokemon' LIKE 'poke\%'`,
		`'poke%mon' LIKE 'poke\%mon'`,
		`'pokemon' LIKE 'poke\%mon'`,
		`'poke%mon' = 'poke%mon'`,
		`'poke\%mon' = 'poke%mon'`,
		`'poke%mon' = 'poke\%mon'`,
		`'poke\%mon' = 'poke\%mon'`,
		`'pokemon' LIKE 'poke_on'`,
		`'pokemon' LIKE 'poke\_on'`,
		`'poke_mon' LIKE 'poke\_mon'`,
		`'pokemon' LIKE 'poke\_mon'`,
		`'poke_mon' = 'poke_mon'`,
		`'poke\_mon' = 'poke_mon'`,
		`'poke_mon' = 'poke\_mon'`,
		`'poke\_mon' = 'poke\_mon'`,
	}
	for _, query := range queries {
		yield(query, nil)
	}
}

func (Types) Test(yield Iterator) {
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
		yield(query, nil)
	}
}

func (HexArithmetic) Test(yield Iterator) {
	var cases = []string{
		`0`, `1`, `1.0`, `0.0`, `1.0e0`, `0.0e0`,
		`X'00'`, `X'1234'`, `X'ff'`,
		`0x00`, `0x1`, `0x1234`,
		`0xff`, `0xffff`, `0xffffffff`, `0xffffffffffffffff`,
	}

	for _, lhs := range cases {
		for _, rhs := range cases {
			yield(fmt.Sprintf("%s + %s", lhs, rhs), nil)
			// compare with negative values too
			yield(fmt.Sprintf("-%s + -%s", lhs, rhs), nil)
		}
	}
}

func (NumericTypes) Test(yield Iterator) {
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
		`18446744073709540000e0`,
	}

	for _, rhs := range numbers {
		yield(rhs, nil)
	}
}

func (NegateArithmetic) Test(yield Iterator) {
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

	for _, rhs := range cases {
		yield(fmt.Sprintf("- %s", rhs), nil)
		yield(fmt.Sprintf("-%s", rhs), nil)
	}
}

func (CollationOperations) Test(yield Iterator) {
	var cases = []string{
		"COLLATION('foobar')",
		"COLLATION(_latin1 'foobar')",
		"COLLATION(_utf8mb4 'foobar' COLLATE utf8mb4_general_ci)",
		"COLLATION('foobar' COLLATE utf8mb4_general_ci)",
		"COLLATION(_latin1 'foobar' COLLATE utf8mb4_general_ci)",
	}

	for _, expr := range cases {
		yield(expr, nil)
	}
}

func (LikeComparison) Test(yield Iterator) {
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

	for _, lhs := range left {
		for _, rhs := range right {
			yield(fmt.Sprintf("%s LIKE %s", lhs, rhs), nil)
		}
	}
}

func (MultiComparisons) Test(yield Iterator) {
	var numbers = []string{
		`0`, `-1`, `1`, `0.0`, `1.0`, `-1.0`, `1.0E0`, `-1.0E0`, `0.0E0`,
		strconv.FormatUint(math.MaxUint64, 10),
		strconv.FormatUint(math.MaxInt64, 10),
		strconv.FormatInt(math.MinInt64, 10),
		`CAST(0 AS UNSIGNED)`,
		`CAST(1 AS UNSIGNED)`,
		`CAST(2 AS UNSIGNED)`,
		`CAST(420 AS UNSIGNED)`,
		`'foobar'`, `'FOOBAR'`,
		`"0"`, `"-1"`, `"1"`,
		`_utf8mb4 'foobar'`, `_utf8mb4 'FOOBAR'`,
		`_binary '0'`, `_binary '-1'`, `_binary '1'`,
		`0x0`, `0x1`, `-0x0`, `-0x1`,
		"_utf8mb4 'Abc' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 'aBC' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 'ǍḄÇ' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 'ÁḆĈ' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 '\uA73A' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 '\uA738' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 '\uAC00' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 '\u326E' COLLATE utf8mb4_0900_as_ci",
		"_utf8mb4 'の東京ノ' COLLATE utf8mb4_0900_as_cs",
		"_utf8mb4 'ノ東京の' COLLATE utf8mb4_0900_as_cs",
		"_utf8mb4 'の東京ノ' COLLATE utf8mb4_ja_0900_as_cs",
		"_utf8mb4 'ノ東京の' COLLATE utf8mb4_ja_0900_as_cs",
		"_utf8mb4 'の東京ノ' COLLATE utf8mb4_ja_0900_as_cs_ks",
		"_utf8mb4 'ノ東京の' COLLATE utf8mb4_ja_0900_as_cs_ks",
	}

	for _, method := range []string{"LEAST", "GREATEST"} {
		genSubsets(numbers, 2, func(arg []string) {
			yield(fmt.Sprintf("%s(%s, %s)", method, arg[0], arg[1]), nil)
			yield(fmt.Sprintf("%s(%s, %s)", method, arg[1], arg[0]), nil)
		})

		genSubsets(numbers, 3, func(arg []string) {
			yield(fmt.Sprintf("%s(%s, %s, %s)", method, arg[0], arg[1], arg[2]), nil)
			yield(fmt.Sprintf("%s(%s, %s, %s)", method, arg[2], arg[1], arg[0]), nil)
		})
	}
}

func (IsStatement) Test(yield Iterator) {
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

	for _, l := range left {
		for _, r := range right {
			yield(fmt.Sprintf("%s IS %s", l, r), nil)
		}
	}
}

func (TupleComparisons) Test(yield Iterator) {
	var elems = []string{"NULL", "-1", "0", "1"}
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}

	var tuples []string
	perm(elems, func(t []string) {
		tuples = append(tuples, "("+strings.Join(t, ", ")+")")
	})

	for _, op := range operators {
		for i := 0; i < len(tuples); i++ {
			for j := 0; j < len(tuples); j++ {
				yield(fmt.Sprintf("%s %s %s", tuples[i], op, tuples[j]), nil)
			}
		}
	}
}

func (Comparisons) Test(yield Iterator) {
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}
	for _, op := range operators {
		for i := 0; i < len(inputComparisonElement); i++ {
			for j := 0; j < len(inputComparisonElement); j++ {
				yield(fmt.Sprintf("%s %s %s", inputComparisonElement[i], op, inputComparisonElement[j]), nil)
			}
		}
	}
}

func (JSONExtract) Test(yield Iterator) {
	var cases = []struct {
		Operator string
		Path     string
	}{
		{Operator: `->>`, Path: "$**.b"},
		{Operator: `->>`, Path: "$.c"},
		{Operator: `->>`, Path: "$.b[1].c"},
		{Operator: `->`, Path: "$.b[1].c"},
		{Operator: `->>`, Path: "$.b[1]"},
		{Operator: `->>`, Path: "$[0][0]"},
		{Operator: `->>`, Path: "$**[0]"},
		{Operator: `->>`, Path: "$.a[0]"},
		{Operator: `->>`, Path: "$[0].a[0]"},
		{Operator: `->>`, Path: "$**.a"},
		{Operator: `->>`, Path: "$[0][0][0].a"},
		{Operator: `->>`, Path: "$[*].b"},
		{Operator: `->>`, Path: "$[*].a"},
	}

	var rows = []sqltypes.Value{
		mustJSON(`[ { "a": 1 }, { "a": 2 } ]`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : 123, "c" : 456 } ] }`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : "123" } ] }`),
		mustJSON(`{ "a" : "foo", "b" : [ true, { "c" : 123 } ] }`),
	}

	for _, tc := range cases {
		expr0 := fmt.Sprintf("column0%s'%s'", tc.Operator, tc.Path)
		expr1 := fmt.Sprintf("cast(json_unquote(json_extract(column0, '%s')) as char)", tc.Path)
		expr2 := fmt.Sprintf("cast(%s as char) <=> %s", expr0, expr1)

		for _, row := range rows {
			yield(expr0, []sqltypes.Value{row})
			yield(expr1, []sqltypes.Value{row})
			yield(expr2, []sqltypes.Value{row})
		}
	}
}

func (JSONExtract) Environment() *evalengine.ExpressionEnv {
	env := new(evalengine.ExpressionEnv)
	env.DefaultCollation = collations.CollationUtf8mb4ID
	env.Fields = []*querypb.Field{
		{
			Name:       "column0",
			Type:       sqltypes.TypeJSON,
			ColumnType: "JSON",
		},
	}
	return env
}

func (FnLower) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("LOWER(%s)", str), nil)
		yield(fmt.Sprintf("LCASE(%s)", str), nil)
	}
}

func (FnUpper) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("UPPER(%s)", str), nil)
		yield(fmt.Sprintf("UCASE(%s)", str), nil)
	}
}

func (FnCharLength) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("CHAR_LENGTH(%s)", str), nil)
		yield(fmt.Sprintf("CHARACTER_LENGTH(%s)", str), nil)
	}
}

func (FnLength) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("LENGTH(%s)", str), nil)
		yield(fmt.Sprintf("OCTET_LENGTH(%s)", str), nil)
	}
}

func (FnBitLength) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("BIT_LENGTH(%s)", str), nil)
	}
}

func (FnAscii) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("ASCII(%s)", str), nil)
	}
}

func (FnRepeat) Test(yield Iterator) {
	counts := []string{"-1", "1.2", "3", "1073741825"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			yield(fmt.Sprintf("repeat(%s, %s)", str, cnt), nil)
		}
	}
}

func (FnHex) Test(yield Iterator) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("hex(%s)", str), nil)
	}

	for _, str := range inputConversions {
		yield(fmt.Sprintf("hex(%s)", str), nil)
	}

	for _, str := range inputBitwise {
		yield(fmt.Sprintf("hex(%s)", str), nil)
	}
}
