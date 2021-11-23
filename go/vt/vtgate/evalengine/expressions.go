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
	"strings"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	EvalResult struct {
		typ          querypb.Type
		ival         int64
		uval         uint64
		fval         float64
		bytes        []byte
		collation    collations.ID
		tupleResults []EvalResult
	}

	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		Evaluate(env ExpressionEnv) (EvalResult, error)
		Type(env ExpressionEnv) (querypb.Type, error)
		String() string
	}

	// Expressions
	Null    struct{}
	Literal struct {
		Val       EvalResult
		Collation collations.ID
	}
	BindVariable struct {
		Key       string
		Collation collations.ID
	}
	Column struct {
		Offset    int
		Collation collations.ID
	}
	Tuple []Expr
)

// Evaluate implements the Expr interface
func (t Tuple) Evaluate(env ExpressionEnv) (EvalResult, error) {
	var res EvalResult
	res.typ = querypb.Type_TUPLE
	for _, expr := range t {
		evalRes, err := expr.Evaluate(env)
		if err != nil {
			return EvalResult{}, err
		}
		res.tupleResults = append(res.tupleResults, evalRes)
	}
	return res, nil
}

// Type implements the Expr interface
func (t Tuple) Type(env ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_TUPLE, nil
}

// String implements the Expr interface
func (t Tuple) String() string {
	var stringSlice []string
	for _, expr := range t {
		stringSlice = append(stringSlice, expr.String())
	}
	return "(" + strings.Join(stringSlice, ",") + ")"
}

var _ Expr = (*Null)(nil)
var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*BinaryExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (Tuple)(nil)

// Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ)
}

// NewLiteralIntFromBytes returns a literal expression
func NewLiteralIntFromBytes(val []byte) (Expr, error) {
	ival, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, err
	}
	return NewLiteralInt(ival), nil
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) Expr {
	return &Literal{Val: EvalResult{typ: sqltypes.Int64, ival: i}}
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	return &Literal{Val: EvalResult{typ: sqltypes.Float64, fval: val}}
}

// NewLiteralFloatFromBytes returns a float literal expression from a slice of bytes
func NewLiteralFloatFromBytes(val []byte) (Expr, error) {
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	return &Literal{Val: EvalResult{typ: sqltypes.Float64, fval: fval}}, nil
}

// NewLiteralString returns a literal expression
func NewLiteralString(val []byte, collation collations.ID) Expr {
	return &Literal{Val: EvalResult{typ: sqltypes.VarBinary, bytes: val}, Collation: collation}
}

// NewBindVar returns a bind variable
func NewBindVar(key string, collation collations.ID) Expr {
	return &BindVariable{
		Key:       key,
		Collation: collation,
	}
}

// NewColumn returns a bind variable
func NewColumn(offset int, collation collations.ID) Expr {
	return &Column{
		Offset:    offset,
		Collation: collation,
	}
}

// Evaluate implements the Expr interface
func (n Null) Evaluate(ExpressionEnv) (EvalResult, error) {
	return EvalResult{}, nil
}

// Type implements the Expr interface
func (n Null) Type(ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_NULL_TYPE, nil
}

// String implements the Expr interface
func (n Null) String() string {
	return "null"
}

// Evaluate implements the Expr interface
func (l *Literal) Evaluate(ExpressionEnv) (EvalResult, error) {
	eval := l.Val
	eval.collation = l.Collation
	return eval, nil
}

// Evaluate implements the Expr interface
func (b *BindVariable) Evaluate(env ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[b.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	eval, err := evaluateByType(val)
	if err != nil {
		return EvalResult{}, err
	}
	eval.collation = b.Collation
	return eval, nil
}

// Evaluate implements the Expr interface
func (c *Column) Evaluate(env ExpressionEnv) (EvalResult, error) {
	value := env.Row[c.Offset]
	numeric, err := newEvalResult(value)
	numeric.collation = c.Collation
	return numeric, err
}

// Type implements the Expr interface
func (b *BindVariable) Type(env ExpressionEnv) (querypb.Type, error) {
	e := env.BindVars
	v, found := e[b.Key]
	if !found {
		return querypb.Type_NULL_TYPE, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", b.Key)
	}
	return v.Type, nil
}

// Type implements the Expr interface
func (l *Literal) Type(ExpressionEnv) (querypb.Type, error) {
	return l.Val.typ, nil
}

// Type implements the Expr interface
func (c *Column) Type(ExpressionEnv) (querypb.Type, error) {
	return sqltypes.Float64, nil
}

// String implements the Expr interface
func (b *BindVariable) String() string {
	return ":" + b.Key
}

// String implements the Expr interface
func (l *Literal) String() string {
	return l.Val.Value().String()
}

// String implements the Expr interface
func (c *Column) String() string {
	return fmt.Sprintf("column %d from the input", c.Offset)
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
		return EvalResult{typ: sqltypes.Int64, ival: ival}, nil
	case sqltypes.Int32:
		ival, err := strconv.ParseInt(string(val.Value), 10, 32)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int32, ival: ival}, nil
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(val.Value), 10, 64)
		if err != nil {
			uval = 0
		}
		return EvalResult{typ: sqltypes.Uint64, uval: uval}, nil
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(val.Value), 64)
		if err != nil {
			fval = 0
		}
		return EvalResult{typ: sqltypes.Float64, fval: fval}, nil
	case sqltypes.VarChar, sqltypes.Text, sqltypes.VarBinary:
		return EvalResult{typ: sqltypes.VarBinary, bytes: val.Value}, nil
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Date:
		return EvalResult{typ: val.Type, bytes: val.Value}, nil
	case sqltypes.Null:
		return EvalResult{typ: sqltypes.Null}, nil
	}
	return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %s", val.Type.String())
}

// debugString prints the entire EvalResult in a debug format
func (e *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) %d %d %f %s", querypb.Type_name[int32(e.typ)], e.ival, e.uval, e.fval, string(e.bytes))
}
