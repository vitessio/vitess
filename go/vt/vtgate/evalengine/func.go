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

var builtinFunctions = map[string]func(*ExpressionEnv, []EvalResult, *EvalResult){
	"coalesce":  builtinFuncCoalesce,
	"greatest":  builtinFuncGreatest,
	"least":     builtinFuncLeast,
	"collation": builtinFuncCollation,
	"isnull":    builtinFuncIsNull,
	"bit_count": builtinFuncBitCount,
}

type CallExpr struct {
	Arguments TupleExpr
	Aliases   []sqlparser.ColIdent
	Method    string
	Call      func(*ExpressionEnv, []EvalResult, *EvalResult)
}

func (c *CallExpr) typeof(*ExpressionEnv) sqltypes.Type {
	return -1
}

func (c *CallExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var args []EvalResult = make([]EvalResult, len(c.Arguments))
	for i, arg := range c.Arguments {
		args[i].init(env, arg)
	}
	c.Call(env, args, result)
}

func builtinFuncCoalesce(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	for _, arg := range args {
		if !arg.null() {
			*result = arg
			result.resolve()
			return
		}
	}
	result.setNull()
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
		switch arg.typeof() {
		case sqltypes.Null:
			return func(args []EvalResult, result *EvalResult, cmp int) {
				result.setNull()
			}
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
	maxFrac := candidateD.frac

	for _, arg := range args[1:] {
		thisD := arg.coerceToDecimal()
		if (cmp < 0) == (thisD.num.Cmp(&candidateD.num) < 0) {
			candidateD = thisD
		}
		if thisD.frac > maxFrac {
			maxFrac = thisD.frac
		}
	}

	candidateD.frac = maxFrac
	result.setDecimal(candidateD)
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

func throwArgError(fname string) {
	panic(evalError{fmt.Errorf("Incorrect parameter count in the call to native function '%s'", fname)})
}

func builtinFuncLeast(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) < 2 {
		throwArgError("LEAST")
	}
	getMultiComparisonFunc(args)(args, result, -1)
}

func builtinFuncGreatest(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) < 2 {
		throwArgError("GREATEST")
	}
	getMultiComparisonFunc(args)(args, result, 1)
}

func builtinFuncCollation(env *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) != 1 {
		throwArgError("COLLATION")
	}

	coll := collations.Local().LookupByID(args[0].collation().Collation)
	result.setString(coll.Name(), collations.TypedCollation{
		Collation:    env.DefaultCollation,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireASCII,
	})
}

func builtinFuncIsNull(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) != 1 {
		throwArgError("ISNULL")
	}
	result.setBool(args[0].null())
}

func builtinFuncBitCount(_ *ExpressionEnv, args []EvalResult, result *EvalResult) {
	if len(args) != 1 {
		throwArgError("BIT_COUNT")
	}

	var count int
	inarg := &args[0]

	if inarg.null() {
		result.setNull()
		return
	}

	if inarg.bitwiseBinaryString() {
		binary := inarg.bytes()
		for _, b := range binary {
			count += bits.OnesCount8(b)
		}
	} else {
		inarg.makeIntegral()
		count = bits.OnesCount64(inarg.uint64())
	}

	result.setInt64(int64(count))
}

type WeightStringCallExpr struct {
	String Expr
	Cast   string
	Len    int
}

func (c *WeightStringCallExpr) typeof(*ExpressionEnv) sqltypes.Type {
	return sqltypes.VarBinary
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
