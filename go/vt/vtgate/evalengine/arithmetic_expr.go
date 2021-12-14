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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	ArithmeticExpr struct {
		BinaryExpr
		Op ArithmeticOp
	}

	// ArithmeticOp allows arithmetic expressions to not have to evaluate child expressions - this is done by the BinaryExpr
	ArithmeticOp interface {
		eval(left, right EvalResult) (EvalResult, error)
		typeof(left querypb.Type) querypb.Type
		String() string
	}

	OpAddition       struct{}
	OpSubstraction   struct{}
	OpMultiplication struct{}
	OpDivision       struct{}
)

var _ ArithmeticOp = (*OpAddition)(nil)
var _ ArithmeticOp = (*OpSubstraction)(nil)
var _ ArithmeticOp = (*OpMultiplication)(nil)
var _ ArithmeticOp = (*OpDivision)(nil)

func (b *ArithmeticExpr) collation() collations.TypedCollation {
	return collationNumeric
}

func (b *ArithmeticExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	lVal, err := b.Left.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	if lVal.null() {
		return resultNull, nil
	}
	rVal, err := b.Right.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	if rVal.null() {
		return resultNull, nil
	}
	if lVal.typ == querypb.Type_TUPLE || rVal.typ == querypb.Type_TUPLE {
		panic("failed to typecheck tuples")
	}
	return b.Op.eval(lVal, rVal)
}

// Type implements the Expr interface
func (b *ArithmeticExpr) typeof(env *ExpressionEnv) (querypb.Type, error) {
	ltype, err := b.Left.typeof(env)
	if err != nil {
		return 0, err
	}
	rtype, err := b.Right.typeof(env)
	if err != nil {
		return 0, err
	}
	typ := mergeNumericalTypes(ltype, rtype)
	return b.Op.typeof(typ), nil
}

func (a *OpAddition) eval(left, right EvalResult) (EvalResult, error) {
	return addNumericWithError(left, right)
}
func (a *OpAddition) typeof(left querypb.Type) querypb.Type { return left }
func (a *OpAddition) String() string                        { return "+" }

func (s *OpSubstraction) eval(left, right EvalResult) (EvalResult, error) {
	return subtractNumericWithError(left, right)
}
func (s *OpSubstraction) typeof(left querypb.Type) querypb.Type { return left }
func (s *OpSubstraction) String() string                        { return "-" }

func (m *OpMultiplication) eval(left, right EvalResult) (EvalResult, error) {
	return multiplyNumericWithError(left, right)
}
func (m *OpMultiplication) typeof(left querypb.Type) querypb.Type { return left }
func (m *OpMultiplication) String() string                        { return "*" }

func (d *OpDivision) eval(left, right EvalResult) (EvalResult, error) {
	return divideNumericWithError(left, right, true)
}
func (d *OpDivision) typeof(querypb.Type) querypb.Type { return sqltypes.Float64 }
func (d *OpDivision) String() string                   { return "/" }
