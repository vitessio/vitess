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

	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CallExpr struct {
	Arguments TupleExpr
	Aliases   []sqlparser.ColIdent
	Method    string
	Call      func(*ExpressionEnv, []EvalResult, *EvalResult)
}

func (c *CallExpr) typeof(*ExpressionEnv) querypb.Type {
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

	for _, arg := range args {
		switch arg.typeof() {
		case querypb.Type_NULL_TYPE:
			return func(args []EvalResult, result *EvalResult, cmp int) {
				result.setNull()
			}
		case querypb.Type_INT8, querypb.Type_INT16, querypb.Type_INT32, querypb.Type_INT64:
			integers++
		case querypb.Type_UINT8, querypb.Type_UINT16, querypb.Type_UINT32, querypb.Type_UINT64:
			if arg.uint64() > math.MaxInt64 {
				decimals++
			} else {
				integers++
			}
		case querypb.Type_FLOAT32, querypb.Type_FLOAT64:
			floats++
		case querypb.Type_DECIMAL:
			decimals++
		case querypb.Type_TEXT, querypb.Type_VARCHAR:
			text++
		case querypb.Type_BLOB, querypb.Type_BINARY, querypb.Type_VARBINARY:
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

	result.setRaw(querypb.Type_VARCHAR, candidateB, collationB)
}

func compareAllBinary(args []EvalResult, result *EvalResult, cmp int) {
	candidateB := args[0].toRawBytes()

	for _, arg := range args[1:] {
		thisB := arg.toRawBytes()
		if (cmp < 0) == (bytes.Compare(thisB, candidateB) < 0) {
			candidateB = thisB
		}
	}

	result.setRaw(querypb.Type_VARBINARY, candidateB, collationBinary)
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
