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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// BinaryOp allows binary expressions to not have to evaluate child expressions - this is done by the BinaryExpr
	BinaryOp interface {
		Evaluate(left, right EvalResult) (EvalResult, error)
		Type(left querypb.Type) querypb.Type
		String() string
	}

	BinaryExpr struct {
		Op          BinaryOp
		Left, Right Expr
	}

	// Binary ops
	Addition       struct{}
	Subtraction    struct{}
	Multiplication struct{}
	Division       struct{}
)

var _ BinaryOp = (*Addition)(nil)
var _ BinaryOp = (*Subtraction)(nil)
var _ BinaryOp = (*Multiplication)(nil)
var _ BinaryOp = (*Division)(nil)

// Evaluate implements the Expr interface
func (b *BinaryExpr) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := b.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := b.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return b.Op.Evaluate(lVal, rVal)
}

// Type implements the Expr interface
func (b *BinaryExpr) Type(env ExpressionEnv) (querypb.Type, error) {
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

// String implements the Expr interface
func (b *BinaryExpr) String() string {
	return b.Left.String() + " " + b.Op.String() + " " + b.Right.String()
}

// Evaluate implements the BinaryOp interface
func (a *Addition) Evaluate(left, right EvalResult) (EvalResult, error) {
	return addNumericWithError(left, right)
}

// Evaluate implements the BinaryOp interface
func (s *Subtraction) Evaluate(left, right EvalResult) (EvalResult, error) {
	return subtractNumericWithError(left, right)
}

// Evaluate implements the BinaryOp interface
func (m *Multiplication) Evaluate(left, right EvalResult) (EvalResult, error) {
	return multiplyNumericWithError(left, right)
}

// Evaluate implements the BinaryOp interface
func (d *Division) Evaluate(left, right EvalResult) (EvalResult, error) {
	return divideNumericWithError(left, right)
}

// Type implements the BinaryOp interface
func (a *Addition) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the BinaryOp interface
func (s *Subtraction) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the BinaryOp interface
func (m *Multiplication) Type(left querypb.Type) querypb.Type {
	return left
}

// Type implements the BinaryOp interface
func (d *Division) Type(querypb.Type) querypb.Type {
	return sqltypes.Float64
}

// String implements the BinaryOp interface
func (a *Addition) String() string {
	return "+"
}

// String implements the BinaryOp interface
func (s *Subtraction) String() string {
	return "-"
}

// String implements the BinaryOp interface
func (m *Multiplication) String() string {
	return "*"
}

// String implements the BinaryOp interface
func (d *Division) String() string {
	return "/"
}
