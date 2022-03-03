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

package evalengine

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var builtinFunctions = map[string]builtin{
	"coalesce":  builtinCoalesce{},
	"greatest":  &builtinMultiComparison{name: "GREATEST", cmp: 1},
	"least":     &builtinMultiComparison{name: "LEAST", cmp: -1},
	"collation": builtinCollation{},
	"bit_count": builtinBitCount{},
	"hex":       builtinHex{},
}

var builtinFunctionsRewrite = map[string]builtinRewrite{
	"isnull": builtinIsNullRewrite,
}

type builtin interface {
	call(*ExpressionEnv, []EvalResult, *EvalResult)
	typeof(*ExpressionEnv, []Expr) (sqltypes.Type, flag)
}

type builtinRewrite func([]Expr, TranslationLookup) (Expr, error)

type CallExpr struct {
	Arguments TupleExpr
	Aliases   []sqlparser.ColIdent
	Method    string
	F         builtin
}

func (c *CallExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	return c.F.typeof(env, c.Arguments)
}

func (c *CallExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var args = make([]EvalResult, len(c.Arguments))
	for i, arg := range c.Arguments {
		args[i].init(env, arg)
	}
	c.F.call(env, args, result)
}

type builtinCoalesce struct{}

func (builtinCoalesce) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	for _, arg := range args {
		if !arg.isNull() {
			*result = arg
			result.resolve()
			return
		}
	}
	result.setNull()
}

func (builtinCoalesce) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	return aggregatedType(env, args), flagNullable
}

type multiComparisonFunc func(args []EvalResult, result *EvalResult, cmp int)

func getMultiComparisonFunc(args []EvalResult) multiComparisonFunc {
	var (
		integers int
		floats   int
		decimals int
		text     int
		binary   int
	)

	/*
		If any argument is NULL, the result is NULL. No comparison is needed.
		If all arguments are integer-valued, they are compared as integers.
		If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a DECIMAL value, they are compared as DECIMAL values.
		If the arguments comprise a mix of numbers and strings, they are compared as strings.
		If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
		In all other cases, the arguments are compared as binary strings.
	*/

	for i := range args {
		arg := &args[i]
		if arg.isNull() {
			return func(args []EvalResult, result *EvalResult, cmp int) {
				result.setNull()
			}
		}

		switch arg.typeof() {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
			integers++
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
			if arg.uint64() > math.MaxInt64 {
				decimals++
			} else {
				integers++
			}
		case sqltypes.Float32, sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		}
	}

	if integers == len(args) {
		return compareAllInteger
	}
	if binary > 0 || text > 0 {
		if binary > 0 {
			return compareAllBinary
		}
		if text > 0 {
			return compareAllText
		}
	} else {
		if floats > 0 {
			return compareAllFloat
		}
		if decimals > 0 {
			return compareAllDecimal
		}
	}
	panic("unexpected argument type")
}

func compareAllInteger(args []EvalResult, result *EvalResult, cmp int) {
	var candidateI = args[0].int64()
	for _, arg := range args[1:] {
		thisI := arg.int64()
		if (cmp < 0) == (thisI < candidateI) {
			candidateI = thisI
		}
	}
	result.setInt64(candidateI)
}

func compareAllFloat(args []EvalResult, result *EvalResult, cmp int) {
	candidateF, err := args[0].coerceToFloat()
	if err != nil {
		throwEvalError(err)
	}

	for _, arg := range args[1:] {
		thisF, err := arg.coerceToFloat()
		if err != nil {
			throwEvalError(err)
		}
		if (cmp < 0) == (thisF < candidateF) {
			candidateF = thisF
		}
	}
	result.setFloat(candidateF)
}

func compareAllDecimal(args []EvalResult, result *EvalResult, cmp int) {
	candidateD := args[0].coerceToDecimal()
	maxFrac := args[0].length_

	for _, arg := range args[1:] {
		thisD := arg.coerceToDecimal()
		if (cmp < 0) == (thisD.Cmp(candidateD) < 0) {
			candidateD = thisD
		}
		if arg.length_ > maxFrac {
			maxFrac = arg.length_
		}
	}

	result.setDecimal(candidateD, maxFrac)
}

func compareAllText(args []EvalResult, result *EvalResult, cmp int) {
	env := collations.Local()
	candidateB := args[0].toRawBytes()
	collationB := args[0].collation()

	for _, arg := range args[1:] {
		thisB := arg.toRawBytes()
		thisTC, coerceLeft, coerceRight, err := env.MergeCollations(arg.collation(), collationB, collations.CoercionOptions{ConvertToSuperset: true, ConvertWithCoercion: true})
		if err != nil {
			throwEvalError(err)
		}

		collation := env.LookupByID(thisTC.Collation)

		var leftB = thisB
		var rightB = candidateB
		if coerceLeft != nil {
			leftB, _ = coerceLeft(nil, leftB)
		}
		if coerceRight != nil {
			rightB, _ = coerceRight(nil, rightB)
		}
		if (cmp < 0) == (collation.Collate(leftB, rightB, false) < 0) {
			candidateB = thisB
		}
	}

	result.setRaw(sqltypes.VarChar, candidateB, collationB)
}

func compareAllBinary(args []EvalResult, result *EvalResult, cmp int) {
	candidateB := args[0].toRawBytes()

	for _, arg := range args[1:] {
		thisB := arg.toRawBytes()
		if (cmp < 0) == (bytes.Compare(thisB, candidateB) < 0) {
			candidateB = thisB
		}
	}

	result.setRaw(sqltypes.VarBinary, candidateB, collationBinary)
}

type argError string

func (err argError) Error() string {
	return fmt.Sprintf("Incorrect parameter count in the call to native function '%s'", string(err))
}

func throwArgError(fname string) {
	panic(evalError{argError(fname)})
}

type builtinMultiComparison struct {
	name string
	cmp  int
}

func (cmp *builtinMultiComparison) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	getMultiComparisonFunc(args)(args, result, cmp.cmp)
}

func (cmp *builtinMultiComparison) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) < 2 {
		throwArgError(cmp.name)
	}

	var (
		integers int
		floats   int
		decimals int
		text     int
		binary   int
		flags    flag
	)

	for _, expr := range args {
		tt, f := expr.typeof(env)
		flags |= f

		switch tt {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
			integers++
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
			if f&flagIntegerOvf != 0 {
				decimals++
			} else {
				integers++
			}
		case sqltypes.Float32, sqltypes.Float64:
			floats++
		case sqltypes.Decimal:
			decimals++
		case sqltypes.Text, sqltypes.VarChar:
			text++
		case sqltypes.Blob, sqltypes.Binary, sqltypes.VarBinary:
			binary++
		}
	}

	if flags&flagNull != 0 {
		return sqltypes.Null, flags
	}
	if integers == len(args) {
		return sqltypes.Int64, flags
	}
	if binary > 0 || text > 0 {
		if binary > 0 {
			return sqltypes.VarBinary, flags
		}
		if text > 0 {
			return sqltypes.VarChar, flags
		}
	} else {
		if floats > 0 {
			return sqltypes.Float64, flags
		}
		if decimals > 0 {
			return sqltypes.Decimal, flags
		}
	}
	panic("unexpected argument type")
}

type builtinCollation struct{}

func (builtinCollation) call(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	coll := collations.Local().LookupByID(args[0].collation().Collation)
	result.setString(coll.Name(), collations.TypedCollation{
		Collation:    env.DefaultCollation,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireASCII,
	})
}

func (builtinCollation) typeof(_ *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("COLLATION")
	}
	return sqltypes.VarChar, 0
}

func builtinIsNullRewrite(args []Expr, lookup TranslationLookup) (Expr, error) {
	if len(args) != 1 {
		return nil, argError("ISNULL")
	}
	return &IsExpr{
		UnaryExpr: UnaryExpr{args[0]},
		Op:        sqlparser.IsNullOp,
		Check:     func(er *EvalResult) bool { return er.isNull() },
	}, nil
}

type builtinBitCount struct{}

func (builtinBitCount) call(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	var count int
	inarg := &args[0]

	if inarg.isNull() {
		result.setNull()
		return
	}

	if inarg.isBitwiseBinaryString() {
		binary := inarg.bytes()
		for _, b := range binary {
			count += bits.OnesCount8(b)
		}
	} else {
		inarg.makeUnsignedIntegral()
		count = bits.OnesCount64(inarg.uint64())
	}

	result.setInt64(int64(count))
}

func (builtinBitCount) typeof(env *ExpressionEnv, args []Expr) (sqltypes.Type, flag) {
	if len(args) != 1 {
		throwArgError("BIT_COUNT")
	}

	_, f := args[0].typeof(env)
	return sqltypes.Int64, f
}

type WeightStringCallExpr struct {
	String Expr
	Cast   string
	Len    int
	HasLen bool
}

func (c *WeightStringCallExpr) typeof(env *ExpressionEnv) (sqltypes.Type, flag) {
	_, f := c.String.typeof(env)
	return sqltypes.VarBinary, f
}

func (c *WeightStringCallExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var (
		str     EvalResult
		tc      collations.TypedCollation
		text    []byte
		weights []byte
		length  = c.Len
	)

	str.init(env, c.String)
	tt := str.typeof()

	switch {
	case sqltypes.IsIntegral(tt):
		// when calling WEIGHT_STRING with an integral value, MySQL returns the
		// internal sort key that would be used in an InnoDB table... we do not
		// support that
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", ErrEvaluatedExprNotSupported, FormatExpr(c)))
	case sqltypes.IsQuoted(tt):
		text = str.bytes()
		tc = str.collation()
	default:
		result.setNull()
		return
	}

	if c.Cast == "binary" {
		tc = collationBinary
		weights = make([]byte, 0, c.Len)
		length = collations.PadToMax
	}

	collation := collations.Local().LookupByID(tc.Collation)
	weights = collation.WeightString(weights, text, length)
	result.setRaw(sqltypes.VarBinary, weights, collationBinary)
}

func aggregatedType(env *ExpressionEnv, expr []Expr) sqltypes.Type {
	var (
		double   int
		decimal  int
		signed   int
		unsigned int

		signedMax   sqltypes.Type
		unsignedMax sqltypes.Type

		bit    int
		year   int
		char   int
		binary int
		json   int

		date      int
		time      int
		timestamp int
		datetime  int

		geometry int
		blob     int
		total    int
	)

	for _, e := range expr {
		tt, _ := e.typeof(env)
		switch tt {
		case sqltypes.Float32, sqltypes.Float64:
			double++
		case sqltypes.Decimal:
			decimal++
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
			signed++
			if tt > signedMax {
				signedMax = tt
			}
		case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
			unsigned++
			if tt > unsignedMax {
				unsignedMax = tt
			}
		case sqltypes.Bit:
			bit++
		case sqltypes.Year:
			year++
		case sqltypes.Char, sqltypes.VarChar, sqltypes.Set, sqltypes.Enum:
			char++
		case sqltypes.Binary, sqltypes.VarBinary:
			binary++
		case sqltypes.TypeJSON:
			json++
		case sqltypes.Date:
			date++
		case sqltypes.Datetime:
			datetime++
		case sqltypes.Time:
			time++
		case sqltypes.Timestamp:
			timestamp++
		case sqltypes.Geometry:
			geometry++
		case sqltypes.Blob:
			blob++
		default:
			continue
		}
		total++
	}

	/*
		If all types are numeric, the aggregated type is also numeric:
			If at least one argument is double precision, the result is double precision.
			Otherwise, if at least one argument is DECIMAL, the result is DECIMAL.
			Otherwise, the result is an integer type (with one exception):
				If all integer types are all signed or all unsigned, the result is the same sign and the precision is the highest of all specified integer types (that is, TINYINT, SMALLINT, MEDIUMINT, INT, or BIGINT).
				If there is a combination of signed and unsigned integer types, the result is signed and the precision may be higher. For example, if the types are signed INT and unsigned INT, the result is signed BIGINT.
				The exception is unsigned BIGINT combined with any signed integer type. The result is DECIMAL with sufficient precision and scale 0.
		If all types are BIT, the result is BIT. Otherwise, BIT arguments are treated similar to BIGINT.
		If all types are YEAR, the result is YEAR. Otherwise, YEAR arguments are treated similar to INT.
		If all types are character string (CHAR or VARCHAR), the result is VARCHAR with maximum length determined by the longest character length of the operands.
		If all types are character or binary string, the result is VARBINARY.
		SET and ENUM are treated similar to VARCHAR; the result is VARCHAR.
		If all types are JSON, the result is JSON.
		If all types are temporal, the result is temporal:
			If all temporal types are DATE, TIME, or TIMESTAMP, the result is DATE, TIME, or TIMESTAMP, respectively.
			Otherwise, for a mix of temporal types, the result is DATETIME.
		If all types are GEOMETRY, the result is GEOMETRY.
		If any type is BLOB, the result is BLOB.
		For all other type combinations, the result is VARCHAR.
		Literal NULL operands are ignored for type aggregation.
	*/

	if bit == total {
		return sqltypes.Bit
	} else if bit > 0 {
		signed += bit
		signedMax = sqltypes.Int64
	}

	if year == total {
		return sqltypes.Year
	} else if year > 0 {
		signed += year
		if sqltypes.Int32 > signedMax {
			signedMax = sqltypes.Int32
		}
	}

	if double+decimal+signed+unsigned == total {
		if double > 0 {
			return sqltypes.Float64
		}
		if decimal > 0 {
			return sqltypes.Decimal
		}
		if signed == total {
			return signedMax
		}
		if unsigned == total {
			return unsignedMax
		}
		if unsignedMax == sqltypes.Uint64 && signed > 0 {
			return sqltypes.Decimal
		}
		// TODO
		return sqltypes.Uint64
	}

	if char == total {
		return sqltypes.VarChar
	}
	if char+binary == total {
		return sqltypes.VarBinary
	}
	if json == total {
		return sqltypes.TypeJSON
	}
	if date+time+timestamp+datetime == total {
		if date == total {
			return sqltypes.Date
		}
		if time == total {
			return sqltypes.Time
		}
		if timestamp == total {
			return sqltypes.Timestamp
		}
		return sqltypes.Datetime
	}
	if geometry == total {
		return sqltypes.Geometry
	}
	if blob > 0 {
		return sqltypes.Blob
	}
	return sqltypes.VarChar
}
