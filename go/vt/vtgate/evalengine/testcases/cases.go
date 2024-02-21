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
	"encoding/hex"
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
	{Run: If},
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
	{Run: StrcmpComparison},
	{Run: MultiComparisons},
	{Run: IntervalStatement},
	{Run: IsStatement},
	{Run: NotStatement},
	{Run: LogicalStatement},
	{Run: TupleComparisons},
	{Run: Comparisons},
	{Run: InStatement},
	{Run: FnInsert},
	{Run: FnLower},
	{Run: FnUpper},
	{Run: FnCharLength},
	{Run: FnLength},
	{Run: FnBitLength},
	{Run: FnAscii},
	{Run: FnReverse},
	{Run: FnSpace},
	{Run: FnOrd},
	{Run: FnRepeat},
	{Run: FnLeft},
	{Run: FnLpad},
	{Run: FnRight},
	{Run: FnRpad},
	{Run: FnLTrim},
	{Run: FnRTrim},
	{Run: FnTrim},
	{Run: FnSubstr},
	{Run: FnLocate},
	{Run: FnReplace},
	{Run: FnConcat},
	{Run: FnConcatWs},
	{Run: FnChar},
	{Run: FnHex},
	{Run: FnUnhex},
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
	{Run: FnNow, Compare: &Comparison{LooseTime: true}},
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
	{Run: FnBin},
	{Run: FnOct},
	{Run: FnMD5},
	{Run: FnSHA1},
	{Run: FnSHA2},
	{Run: FnRandomBytes},
	{Run: FnDateFormat},
	{Run: FnConvertTz},
	{Run: FnDate},
	{Run: FnDayOfMonth},
	{Run: FnDayOfWeek},
	{Run: FnDayOfYear},
	{Run: FnFromUnixtime},
	{Run: FnHour},
	{Run: FnMakedate},
	{Run: FnMaketime},
	{Run: FnMicroSecond},
	{Run: FnMinute},
	{Run: FnMonth},
	{Run: FnMonthName},
	{Run: FnLastDay},
	{Run: FnToDays},
	{Run: FnFromDays},
	{Run: FnTimeToSec},
	{Run: FnQuarter},
	{Run: FnSecond},
	{Run: FnTime},
	{Run: FnUnixTimestamp},
	{Run: FnWeek},
	{Run: FnWeekDay},
	{Run: FnWeekOfYear},
	{Run: FnYear},
	{Run: FnYearWeek},
	{Run: FnInetAton},
	{Run: FnInetNtoa},
	{Run: FnInet6Aton},
	{Run: FnInet6Ntoa},
	{Run: FnIsIPv4},
	{Run: FnIsIPv4Compat},
	{Run: FnIsIPv4Mapped},
	{Run: FnIsIPv6},
	{Run: FnBinToUUID},
	{Run: FnIsUUID},
	{Run: FnUUID},
	{Run: FnUUIDToBin},
	{Run: DateMath},
	{Run: RegexpLike},
	{Run: RegexpInstr},
	{Run: RegexpSubstr},
	{Run: RegexpReplace},
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

func FnBin(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("BIN(%s)", num), nil)
	}
	for _, num := range inputBitwise {
		yield(fmt.Sprintf("BIN(%s)", num), nil)
	}
}

func FnOct(yield Query) {
	for _, num := range radianInputs {
		yield(fmt.Sprintf("OCT(%s)", num), nil)
	}
	for _, num := range inputBitwise {
		yield(fmt.Sprintf("OCT(%s)", num), nil)
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

func If(yield Query) {
	var elements []string
	elements = append(elements, inputBitwise...)
	elements = append(elements, inputComparisonElement...)

	for _, cmpbase := range elements {
		for _, val1 := range elements {
			for _, val2 := range elements {
				yield(fmt.Sprintf("if(%s, %s, %s)", cmpbase, val1, val2), nil)
			}
		}
	}
}

func Base64(yield Query) {
	var inputs = []string{
		`'bGlnaHQgdw=='`,
		`'bGlnaHQgd28='`,
		`'bGlnaHQgd29y'`,
		// MySQL trims whitespace
		`'  \t\r\n  bGlnaHQgd28=  \n \t '`,
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
		`'foobar' as char(12)`, `'foobar' as char(3)`, `'foobar' as binary(12)`, `'foobar' as binary(3)`,
		`'foobar' collate utf8mb4_bin as char(12)`, `'foobar' collate utf8mb4_bin as char(3)`,
		`'foobar' collate binary as char(12)`, `'foobar' collate binary as char(3)`,
		`_latin1 'foobar' as char(12)`, `_latin1 'foobar' as binary(12)`,
		`_binary 'foobar' as char(12)`, `_binary 'foobar' as binary(12)`,
		`1`, `-1`, `9223372036854775807`, `18446744073709551615`, `-9223372036854775808`,
		`1 as char(1)`, `-1 as char(1)`, `9223372036854775807 as char(1)`, `18446744073709551615 as char(1)`, `-9223372036854775808 as char(1)`,
		`1 as char(32)`, `-1 as char(32)`, `9223372036854775807 as char(32)`, `18446744073709551615 as char(32)`, `-9223372036854775808 as char(32)`,
		`1 as binary(1)`, `-1 as binary(1)`, `9223372036854775807 as binary(1)`, `18446744073709551615 as binary(1)`, `-9223372036854775808 as binary(1)`,
		`1 as binary(32)`, `-1 as binary(32)`, `9223372036854775807 as binary(32)`, `18446744073709551615 as binary(32)`, `-9223372036854775808 as binary(32)`,
		`1234.0`, `12340e0`,
		`0x1234`, `0x1234 as char(12)`, `0x1234 as char(2)`,
		`date'2000-01-01'`, `date'2000-01-01' as char(12)`, `date'2000-01-01' as char(2)`, `date'2000-01-01' as binary(12)`, `date'2000-01-01' as binary(2)`,
		`timestamp'2000-01-01 11:22:33'`, `timestamp'2000-01-01 11:22:33' as char(12)`, `timestamp'2000-01-01 11:22:33' as char(2)`, `timestamp'2000-01-01 11:22:33' as binary(12)`, `timestamp'2000-01-01 11:22:33' as binary(2)`,
		`timestamp'2000-01-01 11:22:33.123456'`, `timestamp'2000-01-01 11:22:33.123456' as char(12)`, `timestamp'2000-01-01 11:22:33.123456' as char(2)`, `timestamp'2000-01-01 11:22:33.123456' as binary(12)`, `timestamp'2000-01-01 11:22:33.123456' as binary(2)`,
		`time'-11:22:33'`, `time'-11:22:33' as char(12)`, `time'-11:22:33' as char(2)`, `time'-11:22:33' as binary(12)`, `time'-11:22:33' as binary(2)`,
		`time'11:22:33'`, `time'11:22:33' as char(12)`, `time'11:22:33' as char(2)`, `time'11:22:33' as binary(12)`, `time'11:22:33' as binary(2)`,
		`time'101:22:33'`, `time'101:22:33' as char(12)`, `time'101:22:33' as char(2)`, `time'101:22:33' as binary(12)`, `time'101:22:33' as binary(2)`,
		"cast(0 as json)", "cast(1 as json)",
		"cast(true as json)", "cast(false as json)",
		"cast('{}' as json)", "cast('[]' as json)",
		"cast('null' as json)", "cast('true' as json)", "cast('false' as json)",
		"cast('1' as json)", "cast('2' as json)", "cast('1.1' as json)", "cast('-1.1' as json)",
		"cast('9223372036854775807' as json)", "cast('18446744073709551615' as json)",
		// JSON strings
		"cast('\"foo\"' as json)", "cast('\"bar\"' as json)", "cast('invalid' as json)",
		// JSON binary values
		"cast(_binary' \"foo\"' as json)", "cast(_binary '\"bar\"' as json)",
		"cast(0xFF666F6F626172FF as json)", "cast(0x666F6F626172FF as json)",
		"cast(0b01 as json)", "cast(0b001 as json)",
		// JSON arrays
		"cast('[\"a\"]' as json)", "cast('[\"ab\"]' as json)",
		"cast('[\"ab\", \"cd\", \"ef\"]' as json)", "cast('[\"ab\", \"ef\"]' as json)",
		// JSON objects
		"cast('{\"a\": 1, \"b\": 2}' as json)", "cast('{\"b\": 2, \"a\": 1}' as json)",
		"cast('{\"c\": 1, \"b\": 2}' as json)", "cast('{\"b\": 2, \"c\": 1}' as json)",
		"cast(' \"b\": 2}' as json)", "cast('\"a\": 1' as json)",
		// JSON date, datetime & time
		"cast(date '2000-01-01' as json)", "cast(date '2000-01-02' as json)",
		"cast(timestamp '2000-01-01 12:34:58' as json)",
		"cast(time '12:34:56' as json)", "cast(time '12:34:58' as json)", "cast(time '5 12:34:58' as json)",
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

	for _, rhs := range inputConversions {
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

func StrcmpComparison(yield Query) {
	inputs := append([]string{
		`'foobar'`, `'FOOBAR'`,
		`'1234'`, `1234`,
		`_utf8mb4 'foobar' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOOBAR' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'foobar' COLLATE utf8mb4_0900_as_ci`,
		`_utf8mb4 'FOOBAR' COLLATE utf8mb4_0900_as_ci`,
		`'foo%'`, `'FOO%'`, `'foo_ar'`, `'FOO_AR'`,
		`'12%'`, `'12_4'`, `'12x4'`, `'12$4'`,
		`_utf8mb4 '12_4' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 '12_4' COLLATE utf8mb4_0900_ai_ci`,
		`_utf8mb4 '12x4' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 '12x4' COLLATE utf8mb4_0900_ai_ci`,
		`_utf8mb4 '12$4' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 '12$4' COLLATE utf8mb4_0900_ai_ci`,
		`_utf8mb4 'foo%' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOO%' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'foo_ar' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'FOO_AR' COLLATE utf8mb4_0900_as_cs`,
		`_utf8mb4 'foo%' COLLATE utf8mb4_0900_as_ci`,
		`_utf8mb4 'FOO%' COLLATE utf8mb4_0900_as_ci`,
		`_utf8mb4 'foo_ar' COLLATE utf8mb4_0900_as_ci`,
		`_utf8mb4 'FOO_AR' COLLATE utf8mb4_0900_as_ci`,
	}, inputConversions...)

	for _, lhs := range inputs {
		for _, rhs := range inputs {
			yield(fmt.Sprintf("STRCMP(%s, %s)", lhs, rhs), nil)
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

func IntervalStatement(yield Query) {
	inputs := []string{
		"-1", "0", "1", "2", "3", "0xFF", "1.1", "1.9", "1.1e0", "1.9e0",
		strconv.FormatUint(math.MaxUint64, 10),
		strconv.FormatUint(math.MaxInt64, 10),
		strconv.FormatUint(math.MaxInt64+1, 10),
		strconv.FormatInt(math.MinInt64, 10),
		"18446744073709551616",
		"-9223372036854775809",
		`"foobar"`, "NULL", "cast('invalid' as json)",
	}
	for _, base := range inputs {
		for _, arg1 := range inputs {
			for _, arg2 := range inputs {
				for _, arg3 := range inputs {
					yield(fmt.Sprintf("INTERVAL(%s, %s, %s, %s)", base, arg1, arg2, arg3), nil)
				}
			}
		}
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

func FnInsert(yield Query) {
	for _, s := range insertStrings {
		for _, ns := range insertStrings {
			for _, l := range inputBitwise {
				for _, p := range inputBitwise {
					yield(fmt.Sprintf("INSERT(%s, %s, %s, %s)", s, p, l, ns), nil)
				}
			}
		}
	}

	mysqlDocSamples := []string{
		"INSERT('Quadratic', 3, 4, 'What')",
		"INSERT('Quadratic', -1, 4, 'What')",
		"INSERT('Quadratic', 3, 100, 'What')",
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}
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

func FnReverse(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("REVERSE(%s)", str), nil)
	}
}

func FnSpace(yield Query) {
	counts := []string{
		"0",
		"12",
		"23",
		"-1",
		"-12393128120",
		"-432766734237843674326423876243876234786",
		"'-432766734237843674326423876243876234786'",
		"432766734237843674326423876243876234786",
		"1073741825",
		"1.5",
		"-3.2",
		"'jhgjhg'",
		"6",
	}

	for _, c := range counts {
		yield(fmt.Sprintf("SPACE(%s)", c), nil)
	}
}

func FnOrd(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("ORD(%s)", str), nil)
	}
}

func FnRepeat(yield Query) {
	counts := []string{"-1", "1.9", "3", "1073741825", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			yield(fmt.Sprintf("REPEAT(%s, %s)", str, cnt), nil)
		}
	}
}

func FnLeft(yield Query) {
	counts := []string{"-1", "1.9", "3", "10", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			yield(fmt.Sprintf("LEFT(%s, %s)", str, cnt), nil)
		}
	}
}

func FnLpad(yield Query) {
	counts := []string{"-1", "1.9", "3", "10", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			for _, pad := range inputStrings {
				yield(fmt.Sprintf("LPAD(%s, %s, %s)", str, cnt, pad), nil)
			}
		}
	}
}

func FnRight(yield Query) {
	counts := []string{"-1", "1.9", "3", "10", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			yield(fmt.Sprintf("RIGHT(%s, %s)", str, cnt), nil)
		}
	}
}

func FnRpad(yield Query) {
	counts := []string{"-1", "1.9", "3", "10", "'1.9'"}
	for _, str := range inputStrings {
		for _, cnt := range counts {
			for _, pad := range inputStrings {
				yield(fmt.Sprintf("RPAD(%s, %s, %s)", str, cnt, pad), nil)
			}
		}
	}
}

func FnLTrim(yield Query) {
	for _, str := range inputTrimStrings {
		yield(fmt.Sprintf("LTRIM(%s)", str), nil)
	}
}

func FnRTrim(yield Query) {
	for _, str := range inputTrimStrings {
		yield(fmt.Sprintf("RTRIM(%s)", str), nil)
	}
}

func FnTrim(yield Query) {
	for _, str := range inputTrimStrings {
		yield(fmt.Sprintf("TRIM(%s)", str), nil)
	}

	modes := []string{"LEADING", "TRAILING", "BOTH"}
	for _, str := range inputTrimStrings {
		for _, mode := range modes {
			yield(fmt.Sprintf("TRIM(%s FROM %s)", mode, str), nil)
		}
	}

	for _, str := range inputTrimStrings {
		for _, pat := range inputTrimStrings {
			yield(fmt.Sprintf("TRIM(%s FROM %s)", pat, str), nil)
			for _, mode := range modes {
				yield(fmt.Sprintf("TRIM(%s %s FROM %s)", mode, pat, str), nil)
			}
		}
	}
}

func FnSubstr(yield Query) {
	mysqlDocSamples := []string{
		`SUBSTRING('Quadratically',5)`,
		`SUBSTRING('foobarbar' FROM 4)`,
		`SUBSTRING('Quadratically',5,6)`,
		`SUBSTRING('Sakila', -3)`,
		`SUBSTRING('Sakila', -5, 3)`,
		`SUBSTRING('Sakila' FROM -4 FOR 2)`,
		`SUBSTR('Quadratically',5)`,
		`SUBSTR('foobarbar' FROM 4)`,
		`SUBSTR('Quadratically',5,6)`,
		`SUBSTR('Sakila', -3)`,
		`SUBSTR('Sakila', -5, 3)`,
		`SUBSTR('Sakila' FROM -4 FOR 2)`,
		`MID('Quadratically',5,6)`,
		`MID('Sakila', -5, 3)`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}

	for _, str := range inputStrings {
		for _, i := range radianInputs {
			yield(fmt.Sprintf("SUBSTRING(%s, %s)", str, i), nil)

			for _, j := range radianInputs {
				yield(fmt.Sprintf("SUBSTRING(%s, %s, %s)", str, i, j), nil)
			}
		}
	}
}

func FnLocate(yield Query) {
	mysqlDocSamples := []string{
		`LOCATE('bar', 'foobarbar')`,
		`LOCATE('xbar', 'foobar')`,
		`LOCATE('bar', 'foobarbar', 5)`,
		`INSTR('foobarbar', 'bar')`,
		`INSTR('xbar', 'foobar')`,
		`POSITION('bar' IN 'foobarbar')`,
		`POSITION('xbar' IN 'foobar')`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}

	for _, substr := range locateStrings {
		for _, str := range locateStrings {
			yield(fmt.Sprintf("LOCATE(%s, %s)", substr, str), nil)
			yield(fmt.Sprintf("INSTR(%s, %s)", str, substr), nil)
			yield(fmt.Sprintf("POSITION(%s IN %s)", str, substr), nil)

			for _, i := range radianInputs {
				yield(fmt.Sprintf("LOCATE(%s, %s, %s)", substr, str, i), nil)
			}
		}
	}
}

func FnReplace(yield Query) {
	cases := []string{
		`REPLACE('www.mysql.com', 'w', 'Ww')`,
		// MySQL doesn't do collation matching for replace, only
		// byte equivalence, but make sure to check.
		`REPLACE('straße', 'ss', 'b')`,
		`REPLACE('straße', 'ß', 'b')`,
		// From / to strings are converted into the collation of
		// the input string.
		`REPLACE('fooÿbar', _latin1 0xFF, _latin1 0xFE)`,
		// First occurence is replaced
		`replace('fff', 'ff', 'gg')`,
	}

	for _, q := range cases {
		yield(q, nil)
	}

	for _, substr := range inputStrings {
		for _, str := range inputStrings {
			for _, i := range inputStrings {
				yield(fmt.Sprintf("REPLACE(%s, %s, %s)", substr, str, i), nil)
			}
		}
	}
}

func FnConcat(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("CONCAT(%s)", str), nil)
	}

	for _, str1 := range inputConversions {
		for _, str2 := range inputConversions {
			yield(fmt.Sprintf("CONCAT(%s, %s)", str1, str2), nil)
		}
	}

	for _, str1 := range inputStrings {
		for _, str2 := range inputStrings {
			for _, str3 := range inputStrings {
				yield(fmt.Sprintf("CONCAT(%s, %s, %s)", str1, str2, str3), nil)
			}
		}
	}
}

func FnConcatWs(yield Query) {
	for _, str := range inputStrings {
		yield(fmt.Sprintf("CONCAT_WS(%s, NULL)", str), nil)
	}

	for _, str1 := range inputConversions {
		for _, str2 := range inputStrings {
			for _, str3 := range inputStrings {
				yield(fmt.Sprintf("CONCAT_WS(%s, %s, %s)", str1, str2, str3), nil)
			}
		}
	}

	for _, str1 := range inputStrings {
		for _, str2 := range inputConversions {
			for _, str3 := range inputStrings {
				yield(fmt.Sprintf("CONCAT_WS(%s, %s, %s)", str1, str2, str3), nil)
			}
		}
	}

	for _, str1 := range inputStrings {
		for _, str2 := range inputStrings {
			for _, str3 := range inputConversions {
				yield(fmt.Sprintf("CONCAT_WS(%s, %s, %s)", str1, str2, str3), nil)
			}
		}
	}
}

func FnChar(yield Query) {
	mysqlDocSamples := []string{
		`CHAR(77,121,83,81,'76')`,
		`CHAR(77,77.3,'77.3')`,
		`CHAR(77,121,83,81,'76' USING utf8mb4)`,
		`CHAR(77,77.3,'77.3' USING utf8mb4)`,
		`HEX(CHAR(1,0))`,
		`HEX(CHAR(256))`,
		`HEX(CHAR(1,0,0))`,
		`HEX(CHAR(256*256)`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}

	for _, i1 := range radianInputs {
		for _, i2 := range inputBitwise {
			for _, i3 := range inputConversions {
				yield(fmt.Sprintf("CHAR(%s, %s, %s)", i1, i2, i3), nil)
			}
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

func FnUnhex(yield Query) {
	var inputs = []string{
		`'f'`,
		`'fe'`,
		`'fea'`,
		`'666F6F626172'`,
		// MySQL trims whitespace
		`'  \t\r\n  4f  \n \t '`,
	}

	inputs = append(inputs, inputConversions...)
	for _, input := range inputConversions {
		inputs = append(inputs, "'"+hex.EncodeToString([]byte(input))+"'")
	}

	for _, lhs := range inputs {
		yield(fmt.Sprintf("UNHEX(%s)", lhs), nil)
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
	var buf strings.Builder
	for _, f := range dateFormats {
		buf.WriteByte('%')
		buf.WriteByte(f.c)
		buf.WriteByte(' ')
	}
	format := buf.String()

	for _, d := range inputConversions {
		yield(fmt.Sprintf("DATE_FORMAT(%s, %q)", d, format), nil)
	}
}

func FnConvertTz(yield Query) {
	timezoneInputs := []string{
		"UTC",
		"GMT",
		"America/New_York",
		"America/Los_Angeles",
		"Europe/London",
		"Europe/Amsterdam",
		"+00:00",
		"-00:00",
		"+01:00",
		"-01:00",
		"+02:00",
		"-02:00",
		"+14:00",
		"-13:00",
		"bogus",
	}
	for _, num1 := range inputConversions {
		for _, tzFrom := range timezoneInputs {
			for _, tzTo := range timezoneInputs {
				q := fmt.Sprintf("CONVERT_TZ(%s, '%s', '%s')", num1, tzFrom, tzTo)
				yield(q, nil)
			}
		}
	}
}

func FnDate(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("DATE(%s)", d), nil)
	}
}

func FnDayOfMonth(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("DAYOFMONTH(%s)", d), nil)
		yield(fmt.Sprintf("DAY(%s)", d), nil)
	}
}

func FnDayOfWeek(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("DAYOFWEEK(%s)", d), nil)
	}
}

func FnDayOfYear(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("DAYOFYEAR(%s)", d), nil)
	}
}

func FnFromUnixtime(yield Query) {
	var buf strings.Builder
	for _, f := range dateFormats {
		buf.WriteByte('%')
		buf.WriteByte(f.c)
		buf.WriteByte(' ')
	}
	format := buf.String()

	for _, d := range inputConversions {
		yield(fmt.Sprintf("FROM_UNIXTIME(%s)", d), nil)
		yield(fmt.Sprintf("FROM_UNIXTIME(%s, %q)", d, format), nil)
	}
}

func FnHour(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("HOUR(%s)", d), nil)
	}
}

func FnMakedate(yield Query) {
	for _, y := range inputConversions {
		for _, d := range inputConversions {
			yield(fmt.Sprintf("MAKEDATE(%s, %s)", y, d), nil)
		}
	}
}

func FnMaketime(yield Query) {
	// Don't use inputConversions for minutes as those are simplest
	// and otherwise we explode in test runtime.
	minutes := []string{
		"''", "0", "'3'", "59", "60", "0xFF666F6F626172FF", "18446744073709551615",
	}
	for _, h := range inputConversions {
		for _, m := range minutes {
			for _, s := range inputConversions {
				yield(fmt.Sprintf("MAKETIME(%s, %s, %s)", h, m, s), nil)
			}
		}
	}
}

func FnMicroSecond(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("MICROSECOND(%s)", d), nil)
	}
}

func FnMinute(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("MINUTE(%s)", d), nil)
	}
}

func FnMonth(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("MONTH(%s)", d), nil)
	}
}

func FnMonthName(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("MONTHNAME(%s)", d), nil)
	}
}

func FnLastDay(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("LAST_DAY(%s)", d), nil)
	}

	dates := []string{
		`DATE'2024-02-18'`,
		`DATE'2023-02-01'`,
		`DATE'2100-02-01'`,
		`TIMESTAMP'2020-12-31 23:59:59'`,
		`TIMESTAMP'2025-01-01 00:00:00'`,
		`'2000-02-01'`,
		`'2020-12-31 23:59:59'`,
		`'2025-01-01 00:00:00'`,
		`20250101`,
		`'20250101'`,
	}

	for _, d := range dates {
		yield(fmt.Sprintf("LAST_DAY(%s)", d), nil)
	}
}

func FnToDays(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("TO_DAYS(%s)", d), nil)
	}

	dates := []string{
		`DATE'0000-00-00'`,
		`0`,
		`'0000-00-00'`,
		`DATE'2023-09-03 00:00:00'`,
		`DATE'2023-09-03 07:00:00'`,
		`DATE'0000-00-00 00:00:00'`,
		`950501`,
		`'2007-10-07'`,
		`'2008-10-07'`,
		`'08-10-07'`,
		`'0000-01-01'`,
	}

	for _, d := range dates {
		yield(fmt.Sprintf("TO_DAYS(%s)", d), nil)
	}
}

func FnFromDays(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("FROM_DAYS(%s)", d), nil)
	}

	days := []string{
		"0",
		"1",
		"366",
		"365242",
		"3652424",
		"3652425",
		"3652500",
		"3652499",
		"730669",
	}

	for _, d := range days {
		yield(fmt.Sprintf("FROM_DAYS(%s)", d), nil)
	}
}

func FnTimeToSec(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("TIME_TO_SEC(%s)", d), nil)
	}

	time := []string{
		`0`,
		`'00:00:00'`,
		`'22:23:00'`,
		`'00:39:38'`,
		`TIME'00:39:38'`,
		`TIME'102:39:38'`,
		`TIME'838:59:59'`,
		`TIME'-838:59:59'`,
		`'000220`,
		`'2003-09-03 00:39:38'`,
		`'2003-09-03'`,
	}

	for _, t := range time {
		yield(fmt.Sprintf("TIME_TO_SEC(%s)", t), nil)
	}
}

func FnQuarter(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("QUARTER(%s)", d), nil)
	}
}

func FnSecond(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("SECOND(%s)", d), nil)
	}
}

func FnTime(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("TIME(%s)", d), nil)
	}
	times := []string{
		"'00:00:00'",
		"'asdadsasd'",
		"'312sadd'",
		"'11-12-23'",
		"'0000-11-23'",
		"'0-0-0'",
		"00:00",
		"00:00-00",
		"00:00:0:0:0:0",
		"00::00",
		"12::00",
		"'00000001'",
		"'11116656'",
	}

	for _, d := range times {
		yield(fmt.Sprintf("TIME(%s)", d), nil)
	}
}

func FnUnixTimestamp(yield Query) {
	yield("UNIX_TIMESTAMP()", nil)

	for _, d := range inputConversions {
		yield(fmt.Sprintf("UNIX_TIMESTAMP(%s)", d), nil)
		yield(fmt.Sprintf("UNIX_TIMESTAMP(%s) + 1", d), nil)
	}
}

func FnWeek(yield Query) {
	for i := 0; i < 16; i++ {
		for _, d := range inputConversions {
			yield(fmt.Sprintf("WEEK(%s, %d)", d, i), nil)
		}
	}
	for _, d := range inputConversions {
		yield(fmt.Sprintf("WEEK(%s)", d), nil)
	}
}

func FnWeekDay(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("WEEKDAY(%s)", d), nil)
	}
}

func FnWeekOfYear(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("WEEKOFYEAR(%s)", d), nil)
	}
}

func FnYear(yield Query) {
	for _, d := range inputConversions {
		yield(fmt.Sprintf("YEAR(%s)", d), nil)
	}
}

func FnYearWeek(yield Query) {
	for i := 0; i < 8; i++ {
		for _, d := range inputConversions {
			yield(fmt.Sprintf("YEARWEEK(%s, %d)", d, i), nil)
		}
	}
	for _, d := range inputConversions {
		yield(fmt.Sprintf("YEARWEEK(%s)", d), nil)
	}
}

func FnInetAton(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("INET_ATON(%s)", d), nil)
	}
}

func FnInetNtoa(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("INET_NTOA(%s)", d), nil)
		yield(fmt.Sprintf("INET_NTOA(INET_ATON(%s))", d), nil)
	}
}

func FnInet6Aton(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("INET6_ATON(%s)", d), nil)
	}
}

func FnInet6Ntoa(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("INET6_NTOA(%s)", d), nil)
		yield(fmt.Sprintf("INET6_NTOA(INET6_ATON(%s))", d), nil)
	}
}

func FnIsIPv4(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("IS_IPV4(%s)", d), nil)
	}
}

func FnIsIPv4Compat(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("IS_IPV4_COMPAT(%s)", d), nil)
		yield(fmt.Sprintf("IS_IPV4_COMPAT(INET6_ATON(%s))", d), nil)
	}
}

func FnIsIPv4Mapped(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("IS_IPV4_MAPPED(%s)", d), nil)
		yield(fmt.Sprintf("IS_IPV4_MAPPED(INET6_ATON(%s))", d), nil)
	}
}

func FnIsIPv6(yield Query) {
	for _, d := range ipInputs {
		yield(fmt.Sprintf("IS_IPV6(%s)", d), nil)
	}
}

func FnBinToUUID(yield Query) {
	args := []string{
		"NULL",
		"-1",
		"0",
		"1",
		"2",
		"''",
		"'-1'",
		"'0'",
		"'1'",
		"'2'",
	}
	for _, d := range uuidInputs {
		yield(fmt.Sprintf("BIN_TO_UUID(%s)", d), nil)
	}

	for _, d := range uuidInputs {
		for _, a := range args {
			yield(fmt.Sprintf("BIN_TO_UUID(%s, %s)", d, a), nil)
		}
	}
}

func FnIsUUID(yield Query) {
	for _, d := range uuidInputs {
		yield(fmt.Sprintf("IS_UUID(%s)", d), nil)
	}
}

func FnUUID(yield Query) {
	yield("LENGTH(UUID())", nil)
	yield("COLLATION(UUID())", nil)
	yield("IS_UUID(UUID())", nil)
	yield("LENGTH(UUID_TO_BIN(UUID())", nil)
}

func FnUUIDToBin(yield Query) {
	args := []string{
		"NULL",
		"-1",
		"0",
		"1",
		"2",
		"''",
		"'-1'",
		"'0'",
		"'1'",
		"'2'",
	}
	for _, d := range uuidInputs {
		yield(fmt.Sprintf("UUID_TO_BIN(%s)", d), nil)
	}

	for _, d := range uuidInputs {
		for _, a := range args {
			yield(fmt.Sprintf("UUID_TO_BIN(%s, %s)", d, a), nil)
		}
	}
}

func DateMath(yield Query) {
	dates := []string{
		`DATE'2018-05-01'`,
		`TIMESTAMP'2020-12-31 23:59:59'`,
		`TIMESTAMP'2025-01-01 00:00:00'`,
		`'2018-05-01'`,
		`'2020-12-31 23:59:59'`,
		`'2025-01-01 00:00:00'`,
		`20250101`,
		`'pokemon trainers'`,
		`'20250101'`,
	}
	intervalValues := []string{
		`1`, `'1:1'`, `'1 1:1:1'`, `'-1 10'`, `'1 10'`, `31`, `30`, `'1.999999'`, `1.999`, `'1.999'`,
		`'1:1:1:1'`, `'1:1 1:1'`, `'-1:10'`, `'1:10'`, `1.5`, `1.5000`, `6/4`, `'6/4'`, `1.5e0`, `1.5000e0`,
		`CAST(6/4 AS DECIMAL(3,1))`, `CAST(6/4 AS DECIMAL(3,0))`, `1e0`, `'1.0'`, `'1.0foobar'`,
	}
	mysqlDocSamples := []string{
		`DATE_ADD(DATE'2018-05-01',INTERVAL 1 DAY)`,
		`DATE_SUB(DATE'2018-05-01',INTERVAL 1 YEAR)`,
		`DATE_ADD(TIMESTAMP'2020-12-31 23:59:59', INTERVAL 1 SECOND)`,
		`DATE_ADD(TIMESTAMP'2018-12-31 23:59:59', INTERVAL 1 DAY)`,
		`DATE_ADD(TIMESTAMP'2100-12-31 23:59:59', INTERVAL '1:1' MINUTE_SECOND)`,
		`DATE_SUB(TIMESTAMP'2025-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND)`,
		`DATE_ADD(TIMESTAMP'1900-01-01 00:00:00', INTERVAL '-1 10' DAY_HOUR)`,
		`DATE_SUB(DATE'1998-01-02', INTERVAL 31 DAY)`,
		`DATE_ADD(TIMESTAMP'1992-12-31 23:59:59.000002', INTERVAL '1.999999' SECOND_MICROSECOND)`,
		`DATE_ADD(DATE'2024-03-30', INTERVAL 1 MONTH)`,
		`DATE_ADD(DATE'2024-03-31', INTERVAL 1 MONTH)`,
		`TIMESTAMPADD(MINUTE, 1, '2003-01-02')`,
		`TIMESTAMPADD(WEEK,1,'2003-01-02')`,
		`TIMESTAMPADD(MONTH, 1, DATE '2024-03-30')`,
		`TIMESTAMPADD(MONTH, 1, DATE '2024-03-31')`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}

	for _, d := range dates {
		for _, i := range inputIntervals {
			for _, v := range intervalValues {
				yield(fmt.Sprintf("DATE_ADD(%s, INTERVAL %s %s)", d, v, i), nil)
				yield(fmt.Sprintf("DATE_SUB(%s, INTERVAL %s %s)", d, v, i), nil)
				yield(fmt.Sprintf("TIMESTAMPADD(%v, %s, %s)", i, v, d), nil)
			}
		}
	}
}

func RegexpLike(yield Query) {
	mysqlDocSamples := []string{
		`'Michael!' REGEXP '.*'`,
		`'Michael!' RLIKE '.*'`,
		`'Michael!' NOT REGEXP '.*'`,
		`'Michael!' NOT RLIKE '.*'`,
		`'new*\n*line' REGEXP 'new\\*.\\*line'`,
		`'a' REGEXP '^[a-d]'`,
		`REGEXP_LIKE('CamelCase', 'CAMELCASE')`,
		`REGEXP_LIKE('CamelCase', 'CAMELCASE' COLLATE utf8mb4_0900_as_cs)`,
		`REGEXP_LIKE('abc', 'ABC'`,
		`REGEXP_LIKE('abc', 'ABC', 'c')`,
		`REGEXP_LIKE(1234, 12)`,
		`REGEXP_LIKE(1234, 12, 'c')`,
		`' '  REGEXP '[[:blank:]]'`,
		`'\t' REGEXP '[[:blank:]]'`,
		`' '  REGEXP '[[:space:]]'`,
		`'\t' REGEXP '[[:space:]]'`,
		`_latin1 0xFF regexp _latin1 '[[:lower:]]' COLLATE latin1_bin`,
		`_koi8r  0xFF regexp _koi8r  '[[:lower:]]' COLLATE koi8r_bin`,
		`_latin1 0xFF regexp _latin1 '[[:upper:]]' COLLATE latin1_bin`,
		`_koi8r  0xFF regexp _koi8r  '[[:upper:]]' COLLATE koi8r_bin`,
		`_latin1 0xF7 regexp _latin1 '[[:alpha:]]'`,
		`_koi8r  0xF7 regexp _koi8r  '[[:alpha:]]'`,
		`_latin1'a' regexp _latin1'A' collate latin1_general_ci`,
		`_latin1'a' regexp _latin1'A' collate latin1_bin`,

		`_latin1 'ÿ' regexp _utf8mb4 'ÿ'`,
		`_utf8mb4 'ÿ' regexp _latin1 'ÿ'`,
		`convert('ÿ' as char character set latin1) regexp _utf8mb4 'ÿ'`,
		`_utf8mb4 'ÿ' regexp convert('ÿ' as char character set latin1)`,

		`'a' regexp '\\p{alphabetic}'`,
		`'a' regexp '\\P{alphabetic}'`,
		`'👌🏾regexp '\\p{Emoji}\\p{Emoji_modifier}'`,
		`'a' regexp '\\p{Lowercase_letter}'`,
		`'a' regexp '\\p{Uppercase_letter}'`,
		`'A' regexp '\\p{Lowercase_letter}'`,
		`'A' regexp '\\p{Uppercase_letter}'`,
		`'a' collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}'`,
		`'A' collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}'`,
		`'a' collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}'`,
		`'A' collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}'`,
		`0xff REGEXP 0xff`,
		`0xff REGEXP 0xfe`,
		`cast(time '12:34:58' as json) REGEXP 0xff`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}

	for _, i := range regexInputs {
		for _, p := range regexInputs {
			yield(fmt.Sprintf("%s REGEXP %s", i, p), nil)
			yield(fmt.Sprintf("%s NOT REGEXP %s", i, p), nil)
			for _, m := range regexMatchStrings {
				yield(fmt.Sprintf("REGEXP_LIKE(%s, %s, %s)", i, p, m), nil)
			}
		}
	}
}

func RegexpInstr(yield Query) {
	mysqlDocSamples := []string{
		`REGEXP_INSTR('Michael!', '.*')`,
		`REGEXP_INSTR('new*\n*line', 'new\\*.\\*line')`,
		`REGEXP_INSTR('a', '^[a-d]')`,
		`REGEXP_INSTR('CamelCase', 'CAMELCASE')`,
		`REGEXP_INSTR('CamelCase', 'CAMELCASE' COLLATE utf8mb4_0900_as_cs)`,
		`REGEXP_INSTR('abc', 'ABC'`,
		`REGEXP_INSTR('abc', 'ABC', 'c')`,
		`REGEXP_INSTR('0', '0', 1, 0)`,
		`REGEXP_INSTR(' ', '[[:blank:]]')`,
		`REGEXP_INSTR('\t', '[[:blank:]]')`,
		`REGEXP_INSTR(' ', '[[:space:]]')`,
		`REGEXP_INSTR('\t', '[[:space:]]')`,
		`REGEXP_INSTR(_latin1 0xFF, _latin1 '[[:lower:]]' COLLATE latin1_bin)`,
		`REGEXP_INSTR(_koi8r  0xFF, _koi8r  '[[:lower:]]' COLLATE koi8r_bin)`,
		`REGEXP_INSTR(_latin1 0xFF, _latin1 '[[:upper:]]' COLLATE latin1_bin)`,
		`REGEXP_INSTR(_koi8r  0xFF, _koi8r  '[[:upper:]]' COLLATE koi8r_bin)`,
		`REGEXP_INSTR(_latin1 0xF7, _latin1 '[[:alpha:]]')`,
		`REGEXP_INSTR(_koi8r  0xF7, _koi8r  '[[:alpha:]]')`,
		`REGEXP_INSTR(_latin1'a', _latin1'A' collate latin1_general_ci)`,
		`REGEXP_INSTR(_latin1'a', _latin1'A' collate latin1_bin)`,
		`REGEXP_INSTR('a', '\\p{alphabetic}')`,
		`REGEXP_INSTR('a', '\\P{alphabetic}')`,
		`REGEXP_INSTR('👌🏾, '\\p{Emoji}\\p{Emoji_modifier}')`,
		`REGEXP_INSTR('a', '\\p{Lowercase_letter}')`,
		`REGEXP_INSTR('a', '\\p{Uppercase_letter}')`,
		`REGEXP_INSTR('A', '\\p{Lowercase_letter}')`,
		`REGEXP_INSTR('A', '\\p{Uppercase_letter}')`,
		`REGEXP_INSTR('a', collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}')`,
		`REGEXP_INSTR('A', collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}')`,
		`REGEXP_INSTR('a', collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}')`,
		`REGEXP_INSTR('A', collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}')`,
		`REGEXP_INSTR('dog cat dog', 'dog')`,
		`REGEXP_INSTR('dog cat dog', 'dog', 2)`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 1)`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 1, 0)`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 1, 1)`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 1, 1, 'i')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 1, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 2)`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 0)`,
		`REGEXP_INSTR('dog cat dog', 'dog', 1, 2, 1)`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1, 'i')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1, 'c')`,
		`REGEXP_INSTR('aa aaa aaaa', 'a{2}')`,
		`REGEXP_INSTR('aa aaa aaaa', 'a{4}')`,
		`REGEXP_INSTR(1234, 12)`,
		`REGEXP_INSTR(1234, 12, 1)`,
		`REGEXP_INSTR(1234, 12, 100)`,
		`REGEXP_INSTR(1234, 12, 1, 1)`,
		`REGEXP_INSTR(1234, 12, 1, 1, 1)`,
		`REGEXP_INSTR(1234, 12, 1, 1, 1, 'c')`,
		`REGEXP_INSTR('', ' ', 1000)`,
		`REGEXP_INSTR(' ', ' ', 1000)`,
		`REGEXP_INSTR(NULL, 'DOG', 1, 2, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', NULL, 1, 2, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', NULL, 2, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, NULL, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, NULL, 'c')`,
		`REGEXP_INSTR('dog cat dog', 'DOG', 1, 2, 1, NULL)`,

		`REGEXP_INSTR('dog cat dog', NULL, 1, 2, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', _latin1 'DOG', NULL, 2, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', _latin1 'DOG', 1, NULL, 1, 'c')`,
		`REGEXP_INSTR('dog cat dog', _latin1 'DOG', 1, 2, NULL, 'c')`,
		`REGEXP_INSTR('dog cat dog', _latin1 'DOG', 1, 2, 1, NULL)`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}
}

func RegexpSubstr(yield Query) {
	mysqlDocSamples := []string{
		`REGEXP_SUBSTR('Michael!', '.*')`,
		`REGEXP_SUBSTR('new*\n*line', 'new\\*.\\*line')`,
		`REGEXP_SUBSTR('a', '^[a-d]')`,
		`REGEXP_SUBSTR('CamelCase', 'CAMELCASE')`,
		`REGEXP_SUBSTR('CamelCase', 'CAMELCASE' COLLATE utf8mb4_0900_as_cs)`,
		`REGEXP_SUBSTR('abc', 'ABC'`,
		`REGEXP_SUBSTR(' ', '[[:blank:]]')`,
		`REGEXP_SUBSTR('\t', '[[:blank:]]')`,
		`REGEXP_SUBSTR(' ', '[[:space:]]')`,
		`REGEXP_SUBSTR('\t', '[[:space:]]')`,
		`REGEXP_SUBSTR(_latin1'a', _latin1'A' collate latin1_general_ci)`,
		`REGEXP_SUBSTR(_latin1'a', _latin1'A' collate latin1_bin)`,
		`REGEXP_SUBSTR('a', '\\p{alphabetic}')`,
		`REGEXP_SUBSTR('a', '\\P{alphabetic}')`,
		`REGEXP_SUBSTR('👌🏾, '\\p{Emoji}\\p{Emoji_modifier}')`,
		`REGEXP_SUBSTR('a', '\\p{Lowercase_letter}')`,
		`REGEXP_SUBSTR('a', '\\p{Uppercase_letter}')`,
		`REGEXP_SUBSTR('A', '\\p{Lowercase_letter}')`,
		`REGEXP_SUBSTR('A', '\\p{Uppercase_letter}')`,
		`REGEXP_SUBSTR('a', collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}')`,
		`REGEXP_SUBSTR('A', collate utf8mb4_0900_as_cs regexp '\\p{Lowercase_letter}')`,
		`REGEXP_SUBSTR('a', collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}')`,
		`REGEXP_SUBSTR('A', collate utf8mb4_0900_as_cs regexp '\\p{Uppercase_letter}')`,
		`REGEXP_SUBSTR('dog cat dog', 'dog')`,
		`REGEXP_SUBSTR('dog cat dog', 'dog', 2)`,
		`REGEXP_SUBSTR('dog cat dog', 'dog', 1, 1)`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 1, 'c')`,
		`REGEXP_SUBSTR('dog cat dog', 'dog', 1, 2)`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 2, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 2, 'c')`,
		`REGEXP_SUBSTR('aa aaa aaaa', 'a{2}')`,
		`REGEXP_SUBSTR('aa aaa aaaa', 'a{4}')`,
		`REGEXP_SUBSTR(1234, 12)`,
		`REGEXP_SUBSTR(1234, 12, 1)`,
		`REGEXP_SUBSTR(1234, 12, 100)`,
		`REGEXP_SUBSTR(1234, 12, 1, 1)`,
		`REGEXP_SUBSTR(1234, 12, 1, 1, 'c')`,

		`REGEXP_SUBSTR(NULL, 'DOG', 1, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', NULL, 1, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', NULL, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, NULL, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 1, NULL)`,

		`REGEXP_SUBSTR(NULL, '[', 1, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', '[', NULL, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', '[', 1, NULL, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', '[', 1, 1, NULL)`,

		`REGEXP_SUBSTR('dog cat dog', 'DOG', 0, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', -1, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 100, 1, 'i')`,
		`REGEXP_SUBSTR('dog cat dog', 'DOG', 1, 1, 0)`,

		`REGEXP_SUBSTR(' ', ' ', 1)`,
		`REGEXP_SUBSTR(' ', ' ', 2)`,
		`REGEXP_SUBSTR(' ', ' ', 3)`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}
}

func RegexpReplace(yield Query) {
	mysqlDocSamples := []string{
		`REGEXP_REPLACE('a b c', 'b', 'X')`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 0)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 1)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 2)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2, 0)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2, 1)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2, 2)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 2, 3)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 3, 0)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 3, 1)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 3, 2)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 3, 3)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 4, 0)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 4, 1)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 4, 2)`,
		`REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 4, 3)`,
		`REGEXP_REPLACE('a', '\\p{Lowercase_letter}', 'X')`,
		`REGEXP_REPLACE('a', '\\p{Uppercase_letter}', 'X')`,
		`REGEXP_REPLACE('A', '\\p{Lowercase_letter}', 'X')`,
		`REGEXP_REPLACE('A', '\\p{Uppercase_letter}', 'X')`,
		`REGEXP_REPLACE(1234, 12, 6)`,
		`REGEXP_REPLACE(1234, 12, 6, 1)`,
		`REGEXP_REPLACE(1234, 12, 6, 100)`,
		`REGEXP_REPLACE(1234, 12, 6, 1, 1)`,
		`REGEXP_REPLACE(1234, 12, 6, 1, 1, 'c')`,

		`REGEXP_REPLACE(NULL, 'DOG', 'bar', 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', NULL, 'bar', 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', NULL, 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', 1, NULL, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', 1, 1, NULL)`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', '1', '1', 0)`,

		`REGEXP_REPLACE(NULL, _latin1'DOG', 'bar', 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', NULL, 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', 1, NULL, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', 1, 1, NULL)`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', '1', '1', 0)`,

		`REGEXP_REPLACE(NULL, '[', 'bar', 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', '[', NULL, 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', '[', 'bar', 1, NULL, 'i')`,
		`REGEXP_REPLACE('dog cat dog', '[', 'bar', 1, 1, NULL)`,

		`REGEXP_REPLACE(NULL, _latin1'[', 'bar', 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'[', NULL, 1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'[', 'bar', 1, NULL, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'[', 'bar', 1, 1, NULL)`,

		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', 0, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', -1, 1, 'i')`,
		`REGEXP_REPLACE('', 'DOG', 'bar', -1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', 100, 1, 'i')`,
		`REGEXP_REPLACE('', 'DOG', 'bar', 100, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', 'DOG', 'bar', 1, 1, 0)`,

		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', 0, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', -1, 1, 'i')`,
		`REGEXP_REPLACE('', _latin1'DOG', 'bar', -1, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', 100, 1, 'i')`,
		`REGEXP_REPLACE('', _latin1'DOG', 'bar', 100, 1, 'i')`,
		`REGEXP_REPLACE('dog cat dog', _latin1'DOG', 'bar', 1, 1, 0)`,

		`REGEXP_REPLACE(' ', ' ', 'x', 1)`,
		`REGEXP_REPLACE(' ', ' ', 'x', 2)`,
		`REGEXP_REPLACE(' ', ' ', 'x', 3)`,

		`REGEXP_REPLACE(' ', _latin1' ', 'x', 1)`,
		`REGEXP_REPLACE(' ', _latin1' ', 'x', 2)`,
		`REGEXP_REPLACE(' ', _latin1' ', 'x', 3)`,
	}

	for _, q := range mysqlDocSamples {
		yield(q, nil)
	}
}
