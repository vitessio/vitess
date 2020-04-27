/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	evalResult struct {
		typ   querypb.Type
		ival  int64
		uval  uint64
		fval  float64
		bytes []byte
	}
	//ExpressionEnv contains the environment that the expression
	//evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// EvalResult is used so we don't have to expose all parts of the private struct
	EvalResult = evalResult

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		Evaluate(env ExpressionEnv) (EvalResult, error)
		Type(env ExpressionEnv) querypb.Type
		String() string
	}

	//BinaryExpr allows binary expressions to not have to evaluate child expressions - this is done by the BinaryOp
	BinaryExpr interface {
		Evaluate(left, right EvalResult) (EvalResult, error)
		Type(left querypb.Type) querypb.Type
		String() string
	}

	// Expressions
	Literal      struct{ Val EvalResult }
	BindVariable struct{ Key string }
	Column       struct{ Offset int }
	BinaryOp     struct {
		Expr        BinaryExpr
		Left, Right Expr
	}

	// Binary ops
	Addition       struct{}
	Subtraction    struct{}
	Multiplication struct{}
	Division       struct{}
)

//Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() sqltypes.Value {
	return castFromNumeric(e, e.typ)
}

//NewLiteralInt returns a literal expression
func NewLiteralInt(val []byte) (Expr, error) {
	ival, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, err
	}
	return &Literal{evalResult{typ: sqltypes.Int64, ival: ival}}, nil
}

//NewLiteralFloat returns a literal expression
func NewLiteralFloat(val []byte) (Expr, error) {
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	return &Literal{evalResult{typ: sqltypes.Float64, fval: fval}}, nil
}

//NewLiteralFloat returns a literal expression
func NewLiteralString(val []byte) (Expr, error) {
	return &Literal{evalResult{typ: sqltypes.VarBinary, bytes: val}}, nil
}

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*BinaryOp)(nil)
var _ Expr = (*Column)(nil)

var _ BinaryExpr = (*Addition)(nil)
var _ BinaryExpr = (*Subtraction)(nil)
var _ BinaryExpr = (*Multiplication)(nil)
var _ BinaryExpr = (*Division)(nil)

//Evaluate implements the Expr interface
func (b *BinaryOp) Evaluate(env ExpressionEnv) (EvalResult, error) {
	lVal, err := b.Left.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	rVal, err := b.Right.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	return b.Expr.Evaluate(lVal, rVal)
}

//Evaluate implements the Expr interface
func (l *Literal) Evaluate(ExpressionEnv) (EvalResult, error) {
	return l.Val, nil
}

//Evaluate implements the Expr interface
func (b *BindVariable) Evaluate(env ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[b.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	return evaluateByType(val)
}

//Evaluate implements the Expr interface
func (c *Column) Evaluate(env ExpressionEnv) (EvalResult, error) {
	value := env.Row[c.Offset]
	numeric, err := newEvalResult(value)
	return numeric, err
}

//Evaluate implements the BinaryOp interface
func (a *Addition) Evaluate(left, right EvalResult) (EvalResult, error) {
	return addNumericWithError(left, right)
}

//Evaluate implements the BinaryOp interface
func (s *Subtraction) Evaluate(left, right EvalResult) (EvalResult, error) {
	return subtractNumericWithError(left, right)
}

//Evaluate implements the BinaryOp interface
func (m *Multiplication) Evaluate(left, right EvalResult) (EvalResult, error) {
	return multiplyNumericWithError(left, right)
}

//Evaluate implements the BinaryOp interface
func (d *Division) Evaluate(left, right EvalResult) (EvalResult, error) {
	return divideNumericWithError(left, right)
}

//Type implements the BinaryExpr interface
func (a *Addition) Type(left querypb.Type) querypb.Type {
	return left
}

//Type implements the BinaryExpr interface
func (m *Multiplication) Type(left querypb.Type) querypb.Type {
	return left
}

//Type implements the BinaryExpr interface
func (d *Division) Type(querypb.Type) querypb.Type {
	return sqltypes.Float64
}

//Type implements the BinaryExpr interface
func (s *Subtraction) Type(left querypb.Type) querypb.Type {
	return left
}

//Type implements the Expr interface
func (b *BinaryOp) Type(env ExpressionEnv) querypb.Type {
	ltype := b.Left.Type(env)
	rtype := b.Right.Type(env)
	typ := mergeNumericalTypes(ltype, rtype)
	return b.Expr.Type(typ)
}

//Type implements the Expr interface
func (b *BindVariable) Type(env ExpressionEnv) querypb.Type {
	e := env.BindVars
	return e[b.Key].Type
}

//Type implements the Expr interface
func (l *Literal) Type(ExpressionEnv) querypb.Type {
	return l.Val.typ
}

//Type implements the Expr interface
func (c *Column) Type(ExpressionEnv) querypb.Type {
	return sqltypes.Float64
}

//String implements the BinaryExpr interface
func (d *Division) String() string {
	return "/"
}

//String implements the BinaryExpr interface
func (m *Multiplication) String() string {
	return "*"
}

//String implements the BinaryExpr interface
func (s *Subtraction) String() string {
	return "-"
}

//String implements the BinaryExpr interface
func (a *Addition) String() string {
	return "+"
}

//String implements the Expr interface
func (b *BinaryOp) String() string {
	return b.Left.String() + " " + b.Expr.String() + " " + b.Right.String()
}

//String implements the Expr interface
func (b *BindVariable) String() string {
	return ":" + b.Key
}

//String implements the Expr interface
func (l *Literal) String() string {
	return l.Val.Value().String()
}

//String implements the Expr interface
func (c *Column) String() string {
	return fmt.Sprintf("[%d]", c.Offset)
}

func mergeNumericalTypes(ltype, rtype querypb.Type) querypb.Type {
	switch ltype {
	case sqltypes.Int64:
		if rtype == sqltypes.Uint64 || rtype == sqltypes.Float64 {
			return rtype
		}
	case sqltypes.Uint64:
		if rtype == sqltypes.Float64 {
			return rtype
		}
	}
	return ltype
}

func evaluateByType(val *querypb.BindVariable) (EvalResult, error) {
	switch val.Type {
	case sqltypes.Int64:
		ival, err := strconv.ParseInt(string(val.Value), 10, 64)
		if err != nil {
			ival = 0
		}
		return evalResult{typ: sqltypes.Int64, ival: ival}, nil
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(val.Value), 10, 64)
		if err != nil {
			uval = 0
		}
		return evalResult{typ: sqltypes.Uint64, uval: uval}, nil
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(val.Value), 64)
		if err != nil {
			fval = 0
		}
		return evalResult{typ: sqltypes.Float64, fval: fval}, nil
	case sqltypes.VarChar, sqltypes.Text, sqltypes.VarBinary:
		return evalResult{typ: sqltypes.VarBinary, bytes: val.Value}, nil
	}
	return evalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %s", val.Type.String())
}
