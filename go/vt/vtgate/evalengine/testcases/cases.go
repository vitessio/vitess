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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var Cases = []TestCase{
	{Run: JSONExtract, Schema: JSONExtract_Schema},
	{Run: JSONPathOperations},
	{Run: JSONArray},
	{Run: JSONObject},
	{Run: CharsetConversionOperators},
	{Run: CaseExprWithPredicate},
	{Run: CaseExprWithValue},
	{Run: Base64},
	{Run: Conversion},
	{Run: LargeDecimals},
	{Run: LargeIntegers},
	{Run: DecimalClamping},
	{Run: BitwiseOperatorsUnary},
	{Run: BitwiseOperators},
	{Run: WeightString},
	{Run: FloatFormatting},
	{Run: UnderscoreAndPercentage},
	{Run: Types},
	{Run: Arithmetic},
	{Run: HexArithmetic},
	{Run: NumericTypes},
	{Run: NegateArithmetic},
	{Run: CollationOperations},
	{Run: LikeComparison},
	{Run: MultiComparisons},
	{Run: IsStatement},
	{Run: NotStatement},
	{Run: LogicalStatement},
	{Run: TupleComparisons},
	{Run: Comparisons},
	{Run: InStatement},
	{Run: FnLower},
	{Run: FnUpper},
	{Run: FnCharLength},
	{Run: FnLength},
	{Run: FnBitLength},
	{Run: FnAscii},
	{Run: FnRepeat},
	{Run: FnHex},
	{Run: FnCeil},
	{Run: FnFloor},
	{Run: FnAbs},
	{Run: FnPi},
	{Run: FnAcos},
	{Run: FnAsin},
	{Run: FnAtan},
	{Run: FnAtan2},
	{Run: FnCos},
	{Run: FnCot},
	{Run: FnSin},
	{Run: FnTan},
	{Run: FnDegrees},
	{Run: FnRadians},
	{Run: FnNow},
	{Run: FnInfo},
	{Run: FnExp},
	{Run: FnLn},
	{Run: FnLog},
	{Run: FnLog10},
	{Run: FnMod},
	{Run: FnLog2},
	{Run: FnPow},
	{Run: FnSign},
	{Run: FnSqrt},
	{Run: FnRound},
	{Run: FnTruncate},
	{Run: FnCrc32},
	{Run: FnConv},
	{Run: FnMD5},
	{Run: FnSHA1},
	{Run: FnSHA2},
	{Run: FnRandomBytes},
	{Run: FnDateFormat},
}

func JSONPathOperations(yield Query) {
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

func JSONArray(yield Query) {
	for _, a := range inputJSONPrimitives {
		yield(fmt.Sprintf("JSON_ARRAY(%s)", a), nil)
		for _, b := range inputJSONPrimitives {
			yield(fmt.Sprintf("JSON_ARRAY(%s, %s)", a, b), nil)
		}
	}
	yield("JSON_ARRAY()", nil)
}

func JSONObject(yield Query) {
	for _, a := range inputJSONPrimitives {
		for _, b := range inputJSONPrimitives {
			yield(fmt.Sprintf("JSON_OBJECT(%s, %s)", a, b), nil)
		}
	}
	yield("JSON_OBJECT()", nil)
}

func CharsetConversionOperators(yield Query) {
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

func CaseExprWithPredicate(yield Query) {
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

func FnCeil(yield Query) {
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

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("CEIL(%s)", num), nil)
		yield(fmt.Sprintf("CEILING(%s)", num), nil)
	}
}

func FnFloor(yield Query) {
	var floorInputs = []string{
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

	for _, num := range floorInputs {
		yield(fmt.Sprintf("FLOOR(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("FLOOR(%s)", num), nil)
	}
}

func FnAbs(yield Query) {
	var absInputs = []string{
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

	for _, num := range absInputs {
		yield(fmt.Sprintf("ABS(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("ABS(%s)", num), nil)
	}
}

func FnPi(yield Query) {
	yield("PI()+0.000000000000000000", nil)
}

func FnAcos(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("ACOS(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("ACOS(%s)", num), nil)
	}
}

func FnAsin(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("ASIN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("ASIN(%s)", num), nil)
	}
}

func FnAtan(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("ATAN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("ATAN(%s)", num), nil)
	}
}

func FnAtan2(yield Query) {
	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("ATAN(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("ATAN2(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("ATAN(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("ATAN2(%s, %s)", num1, num2), nil)
		}
	}
}

func FnCos(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("COS(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("COS(%s)", num), nil)
	}
}

func FnCot(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("COT(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("COT(%s)", num), nil)
	}
}

func FnSin(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("SIN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("SIN(%s)", num), nil)
	}
}

func FnTan(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("TAN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("TAN(%s)", num), nil)
	}
}

func FnDegrees(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("DEGREES(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("DEGREES(%s)", num), nil)
	}
}

func FnRadians(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("RADIANS(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("RADIANS(%s)", num), nil)
	}
}

func FnExp(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("EXP(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("EXP(%s)", num), nil)
	}
}

func FnLn(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("LN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("LN(%s)", num), nil)
	}
}

func FnLog(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("LOG(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("LOG(%s)", num), nil)
	}

	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("LOG(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("LOG(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("LOG(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("LOG(%s, %s)", num1, num2), nil)
		}
	}
}

func FnLog10(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("LOG10(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("LOG10(%s)", num), nil)
	}
}

func FnMod(yield Query) {
	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("MOD(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("MOD(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("MOD(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("MOD(%s, %s)", num1, num2), nil)
		}
	}
}

func FnLog2(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("LOG2(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("LOG2(%s)", num), nil)
	}
}

func FnPow(yield Query) {
	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("POW(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("POWER(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("POW(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("POWER(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("POW(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("POWER(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("POW(%s, %s)", num1, num2), nil)
			yield(fmt.Sprintf("POWER(%s, %s)", num1, num2), nil)
		}
	}
}

func FnSign(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("SIGN(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("SIGN(%s)", num), nil)
	}
}

func FnSqrt(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("SQRT(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("SQRT(%s)", num), nil)
	}
}

func FnRound(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("ROUND(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("ROUND(%s)", num), nil)
	}

	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("ROUND(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("ROUND(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("ROUND(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("ROUND(%s, %s)", num1, num2), nil)
		}
	}
}

func FnTruncate(yield Query) {
	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("TRUNCATE(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("TRUNCATE(%s, %s)", num1, num2), nil)
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range radianInputs {
			yield(fmt.Sprintf("TRUNCATE(%s, %s)", num1, num2), nil)
		}
		for _, num2 := range inputBitwise {
			yield(fmt.Sprintf("TRUNCATE(%s, %s)", num1, num2), nil)
		}
	}
}

func FnCrc32(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("CRC32(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("CRC32(%s)", num), nil)
	}

	for _, num := range inputConversions {
		yield(fmt.Sprintf("CRC32(%s)", num), nil)
	}
}

func FnConv(yield Query) {
	for _, num1 := range radianInputs {
		for _, num2 := range radianInputs {
			for _, num3 := range radianInputs {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
			for _, num3 := range inputBitwise {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
		}
	}

	for _, num1 := range radianInputs {
		for _, num2 := range inputBitwise {
			for _, num3 := range radianInputs {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
			for _, num3 := range inputBitwise {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
		}
	}

	for _, num1 := range inputBitwise {
		for _, num2 := range inputBitwise {
			for _, num3 := range radianInputs {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
			for _, num3 := range inputBitwise {
				yield(fmt.Sprintf("CONV(%s, %s, %s)", num1, num2, num3), nil)
			}
		}
	}
}

func FnMD5(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("MD5(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("MD5(%s)", num), nil)
	}

	for _, num := range inputConversions {
		yield(fmt.Sprintf("MD5(%s)", num), nil)
	}
}

func FnSHA1(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("SHA1(%s)", num), nil)
		yield(fmt.Sprintf("SHA(%s)", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("SHA1(%s)", num), nil)
		yield(fmt.Sprintf("SHA(%s)", num), nil)
	}

	for _, num := range inputConversions {
		yield(fmt.Sprintf("SHA1(%s)", num), nil)
		yield(fmt.Sprintf("SHA(%s)", num), nil)
	}
}

func FnSHA2(yield Query) {
	bitLengths := []string{"0", "224", "256", "384", "512", "1", "0.1", "256.1e0", "1-1", "128+128"}
	for _, bits := range bitLengths {
		for _, num := range radianInputs {
			yield(fmt.Sprintf("SHA2(%s, %s)", num, bits), nil)
		}

		for _, num := range inputBitwise {
			yield(fmt.Sprintf("SHA2(%s, %s)", num, bits), nil)
		}

		for _, num := range inputConversions {
			yield(fmt.Sprintf("SHA2(%s, %s)", num, bits), nil)
		}
	}
}

func FnRandomBytes(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("LENGTH(RANDOM_BYTES(%s))", num), nil)
		yield(fmt.Sprintf("COLLATION(RANDOM_BYTES(%s))", num), nil)
	}

	for _, num := range inputBitwise {
		yield(fmt.Sprintf("LENGTH(RANDOM_BYTES(%s))", num), nil)
		yield(fmt.Sprintf("COLLATION(RANDOM_BYTES(%s))", num), nil)
	}
}

func CaseExprWithValue(yield Query) {
	var elements []string
	elements = append(elements, inputBitwise...)
	elements = append(elements, inputComparisonElement...)

	for _, cmpbase := range elements {
		for _, val1 := range elements {
			if !(bugs{}).CanCompare(cmpbase, val1) {
				continue
			}
			yield(fmt.Sprintf("case %s when %s then 1 else 0 end", cmpbase, val1), nil)
		}
	}
}

func Base64(yield Query) {
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

func Conversion(yield Query) {
	for _, lhs := range inputConversions {
		for _, rhs := range inputConversionTypes {
			yield(fmt.Sprintf("CAST(%s AS %s)", lhs, rhs), nil)
			yield(fmt.Sprintf("CONVERT(%s, %s)", lhs, rhs), nil)
			yield(fmt.Sprintf("CAST(CAST(%s AS JSON) AS %s)", lhs, rhs), nil)
		}
	}
}

func LargeDecimals(yield Query) {
	var largepi = inputPi + inputPi

	for pos := 0; pos < len(largepi); pos++ {
		yield(fmt.Sprintf("%s.%s", largepi[:pos], largepi[pos:]), nil)
		yield(fmt.Sprintf("-%s.%s", largepi[:pos], largepi[pos:]), nil)
	}
}

func LargeIntegers(yield Query) {
	var largepi = inputPi + inputPi

	for pos := 1; pos < len(largepi); pos++ {
		yield(largepi[:pos], nil)
		yield(fmt.Sprintf("-%s", largepi[:pos]), nil)
	}
}

func DecimalClamping(yield Query) {
	for pos := 0; pos < len(inputPi); pos++ {
		for m := 0; m < min(len(inputPi), 67); m += 2 {
			for d := 0; d <= min(m, 33); d += 2 {
				yield(fmt.Sprintf("CAST(%s.%s AS DECIMAL(%d, %d))", inputPi[:pos], inputPi[pos:], m, d), nil)
			}
		}
	}
}

func BitwiseOperatorsUnary(yield Query) {
	for _, op := range []string{"~", "BIT_COUNT"} {
		for _, rhs := range inputBitwise {
			yield(fmt.Sprintf("%s(%s)", op, rhs), nil)
		}
	}
}

func BitwiseOperators(yield Query) {
	for _, op := range []string{"&", "|", "^", "<<", ">>"} {
		for _, lhs := range inputBitwise {
			for _, rhs := range inputBitwise {
				yield(fmt.Sprintf("%s %s %s", lhs, op, rhs), nil)
			}
		}

		for _, lhs := range inputConversions {
			for _, rhs := range inputConversions {
				yield(fmt.Sprintf("%s %s %s", lhs, op, rhs), nil)
			}
		}
	}
}

func WeightString(yield Query) {
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

func FloatFormatting(yield Query) {
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

func UnderscoreAndPercentage(yield Query) {
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

func Types(yield Query) {
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

func Arithmetic(yield Query) {
	operators := []string{"+", "-", "*", "/", "DIV", "%", "MOD"}

	for _, op := range operators {
		for _, lhs := range inputConversions {
			for _, rhs := range inputConversions {
				yield(fmt.Sprintf("%s %s %s", lhs, op, rhs), nil)
			}
		}

		for _, lhs := range inputBitwise {
			for _, rhs := range inputBitwise {
				yield(fmt.Sprintf("%s %s %s", lhs, op, rhs), nil)
			}
		}
	}
}

func HexArithmetic(yield Query) {
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

func NumericTypes(yield Query) {
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

func NegateArithmetic(yield Query) {
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

func CollationOperations(yield Query) {
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

func LikeComparison(yield Query) {
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

func MultiComparisons(yield Query) {
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

func IsStatement(yield Query) {
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

func NotStatement(yield Query) {
	var ops = []string{"NOT", "!"}
	for _, op := range ops {
		for _, i := range inputConversions {
			yield(fmt.Sprintf("%s %s", op, i), nil)
		}
	}
}

func LogicalStatement(yield Query) {
	var ops = []string{"AND", "&&", "OR", "||", "XOR"}
	for _, op := range ops {
		for _, l := range inputConversions {
			for _, r := range inputConversions {
				yield(fmt.Sprintf("%s %s %s", l, op, r), nil)
			}
		}
	}
}

func TupleComparisons(yield Query) {
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

func Comparisons(yield Query) {
	var operators = []string{"=", "!=", "<=>", "<", "<=", ">", ">="}
	for _, op := range operators {
		for _, l := range inputComparisonElement {
			for _, r := range inputComparisonElement {
				yield(fmt.Sprintf("%s %s %s", l, op, r), nil)
			}
		}

		for _, l := range inputConversions {
			for _, r := range inputConversions {
				yield(fmt.Sprintf("%s %s %s", l, op, r), nil)
			}
		}
	}
}

func JSONExtract(yield Query) {
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

var JSONExtract_Schema = []*querypb.Field{
	{
		Name:       "column0",
		Type:       sqltypes.TypeJSON,
		ColumnType: "JSON",
	},
}

func FnLower(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("LOWER(%s)", str), nil)
		yield(fmt.Sprintf("LCASE(%s)", str), nil)
	}
}

func FnUpper(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("UPPER(%s)", str), nil)
		yield(fmt.Sprintf("UCASE(%s)", str), nil)
	}
}

func FnCharLength(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("CHAR_LENGTH(%s)", str), nil)
		yield(fmt.Sprintf("CHARACTER_LENGTH(%s)", str), nil)
	}
}

func FnLength(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("LENGTH(%s)", str), nil)
		yield(fmt.Sprintf("OCTET_LENGTH(%s)", str), nil)
	}
}

func FnBitLength(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("BIT_LENGTH(%s)", str), nil)
	}
}

func FnAscii(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("ASCII(%s)", str), nil)
	}
}

func FnRepeat(yield Query) {
	counts := []string{"-1", "1.9", "3", "1073741825", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			yield(fmt.Sprintf("repeat(%s, %s)", str, cnt), nil)
		}
	}
}

func FnHex(yield Query) {
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

func InStatement(yield Query) {
	roots := append([]string(nil), inputBitwise...)
	roots = append(roots, inputComparisonElement...)

	genSubsets(roots, 3, func(inputs []string) {
		if !(bugs{}).CanCompare(inputs...) {
			return
		}
		yield(fmt.Sprintf("%s IN (%s, %s)", inputs[0], inputs[1], inputs[2]), nil)
		yield(fmt.Sprintf("%s IN (%s, %s)", inputs[2], inputs[1], inputs[0]), nil)
		yield(fmt.Sprintf("%s IN (%s, %s)", inputs[1], inputs[0], inputs[2]), nil)
		yield(fmt.Sprintf("%s IN (%s, %s, %s)", inputs[0], inputs[1], inputs[2], inputs[0]), nil)

		yield(fmt.Sprintf("%s NOT IN (%s, %s)", inputs[0], inputs[1], inputs[2]), nil)
		yield(fmt.Sprintf("%s NOT IN (%s, %s)", inputs[2], inputs[1], inputs[0]), nil)
		yield(fmt.Sprintf("%s NOT IN (%s, %s)", inputs[1], inputs[0], inputs[2]), nil)
		yield(fmt.Sprintf("%s NOT IN (%s, %s, %s)", inputs[0], inputs[1], inputs[2], inputs[0]), nil)
	})
}

func FnNow(yield Query) {
	fns := []string{
		"NOW()", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP",
		"NOW(1)", "CURRENT_TIMESTAMP(1)",
		"LOCALTIME()", "LOCALTIME", "LOCALTIMESTAMP()", "LOCALTIMESTAMP",
		"LOCALTIME(1)", "LOCALTIMESTAMP(1)",
		"UTC_TIMESTAMP()", "UTC_TIMESTAMP",
		"UTC_TIMESTAMP(1)",
		"CURDATE()", "CURRENT_DATE()", "CURRENT_DATE",
		"UTC_TIME()", "UTC_TIME",
		"UTC_DATE()", "UTC_DATE",
		"UTC_TIME(1)",
		"CURTIME()", "CURRENT_TIME()", "CURRENT_TIME",
		"CURTIME(1)", "CURRENT_TIME(1)",
		"SYSDATE()", "SYSDATE(1)",
		"NOW(1)", "NOW(2)", "NOW(3)", "NOW(4)", "NOW(5)",
		"SYSDATE(1)", "SYSDATE(2)", "SYSDATE(3)", "SYSDATE(4)", "SYSDATE(5)",
	}
	for _, fn := range fns {
		yield(fn, nil)
	}
}

func FnInfo(yield Query) {
	fns := []string{
		"USER()", "CURRENT_USER()", "CURRENT_USER",
		"SESSION_USER()", "SYSTEM_USER()",
		"DATABASE()", "SCHEMA()",
		"VERSION()",
	}
	for _, fn := range fns {
		yield(fn, nil)
	}
}

func FnDateFormat(yield Query) {
	formats := []struct {
		c    byte
		expr string
	}{
		{'a', "LEFT(DAYNAME(d),3)"},
		{'b', "LEFT(MONTHNAME(d),3)"},
		{'c', "MONTH(d)"},
		{'D', ""},
		{'d', "LPAD(DAYOFMONTH(d),0,2)"},
		{'e', "DAYOFMONTH(d)"},
		{'f', "LPAD(MICROSECOND(t),6,0)"},
		{'H', "LPAD(HOUR(t),2,0)"},
		{'h', ""},
		{'I', ""},
		{'i', "LPAD(MINUTE(t),2,0)"},
		{'j', ""},
		{'k', "HOUR(t)"},
		{'l', ""},
		{'M', "MONTHNAME(d)"},
		{'m', "LPAD(MONTH(d),2,0)"},
		{'p', ""},
		{'r', ""},
		{'S', "LPAD(SECOND(t),2,0)"},
		{'s', "LPAD(SECOND(t),2,0)"},
		{'T', ""},
		// TODO
		// {'U', "LPAD(WEEK(d,0),2,0)"},
		// {'u', "LPAD(WEEK(d,1),2,0)"},
		// {'V', "RIGHT(YEARWEEK(d,2),2)"},
		// {'v', "RIGHT(YEARWEEK(d,3),2)"},
		{'W', "DAYNAME(d)"},
		{'w', "DAYOFWEEK(d)-1"},
		// TODO
		// {'X', "LEFT(YEARWEEK(d,2),4)"},
		// {'x', "LEFT(YEARWEEK(d,3),4)"},
		{'Y', "YEAR(d)"},
		{'y', "RIGHT(YEAR(d),2)"},
		{'%', ""},
	}

	dates := []string{
		`TIMESTAMP '1999-12-31 23:59:58.999'`,
		`TIMESTAMP '2000-01-02 03:04:05'`,
	}

	var buf strings.Builder
	for _, f := range formats {
		buf.WriteByte('%')
		buf.WriteByte(f.c)
		buf.WriteByte(' ')
	}
	format := buf.String()

	for _, d := range dates {
		yield(fmt.Sprintf("DATE_FORMAT(%s, %q)", d, format), nil)
	}
}
