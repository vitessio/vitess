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
		Evaluate(left, right EvalResult) (EvalResult, error)
		Type(left querypb.Type) querypb.Type
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

func (b *ArithmeticExpr) Collation() collations.TypedCollation {
	return collationNumeric
}

func (b *ArithmeticExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	lVal, err := b.Left.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := b.Right.eval(env)
	if err != nil {
		return EvalResult{}, err
	}
	if lVal.typ == querypb.Type_TUPLE || rVal.typ == querypb.Type_TUPLE {
		return EvalResult{}, cardinalityError(1)
	}
	if hasNullEvalResult(lVal, rVal) {
		return resultNull, nil
	}
	return b.Op.Evaluate(lVal, rVal)
}

// Type implements the Expr interface
func (b *ArithmeticExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	ltype, err := b.Left.Type(env)
	if err != nil {
		return 0, err
	}
	rtype, err := b.Right.Type(env)
	if err != nil {
		return 0, err
	}
	typ := mergeNumericalTypes(ltype, rtype)
	return b.Op.Type(typ), nil
}

// Evaluate implements the ArithmeticOp interface
func (a *OpAddition) Evaluate(left, right EvalResult) (EvalResult, error) {
	return addNumericWithError(left, right)
}

// Evaluate implements the ArithmeticOp interface
func (s *OpSubstraction) Evaluate(left, right EvalResult) (EvalResult, error) {
	return subtractNumericWithError(left, right)
}

// Evaluate implements the ArithmeticOp interface
func (m *OpMultiplication) Evaluate(left, right EvalResult) (EvalResult, error) {
	return multiplyNumericWithError(left, right)
}

// Evaluate implements the ArithmeticOp interface
func (d *OpDivision) Evaluate(left, right EvalResult) (EvalResult, error) {
	return divideNumericWithError(left, right, true)
}

// Type implements the ArithmeticOp interface
func (a *OpAddition) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the ArithmeticOp interface
func (s *OpSubstraction) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the ArithmeticOp interface
func (m *OpMultiplication) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the ArithmeticOp interface
func (d *OpDivision) Type(querypb.Type) querypb.Type {
	return sqltypes.Float64
}

// String implements the ArithmeticOp interface
func (a *OpAddition) String() string {
	return "+"
}

// String implements the ArithmeticOp interface
func (s *OpSubstraction) String() string {
	return "-"
}

// String implements the ArithmeticOp interface
func (m *OpMultiplication) String() string {
	return "*"
}

// String implements the ArithmeticOp interface
func (d *OpDivision) String() string {
	return "/"
}
