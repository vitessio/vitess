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
	"bytes"
	"math"
	"strconv"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		eval(env *ExpressionEnv) (EvalResult, error)
		typeof(env *ExpressionEnv) (querypb.Type, error)
		collation() collations.TypedCollation
		cardinality(env *ExpressionEnv) (int, error)
		format(buf *formatter, depth int)
		constant() bool
		simplify() error
	}

	Literal struct {
		Val EvalResult
	}

	BindVariable struct {
		Key  string
		coll collations.TypedCollation
	}

	Column struct {
		Offset int
		coll   collations.TypedCollation
	}

	TupleExpr []Expr

	CollateExpr struct {
		UnaryExpr
		TypedCollation collations.TypedCollation
	}

	UnaryExpr struct {
		Inner Expr
	}

	BinaryExpr struct {
		Left, Right Expr
	}
)

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*ArithmeticExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (*InExpr)(nil)
var _ Expr = (*LikeExpr)(nil)
var _ Expr = (TupleExpr)(nil)
var _ Expr = (*CollateExpr)(nil)
var _ Expr = (*LogicalExpr)(nil)
var _ Expr = (*NotExpr)(nil)

var noenv *ExpressionEnv = nil

func (env *ExpressionEnv) Evaluate(expr Expr) (EvalResult, error) {
	_, err := expr.cardinality(env)
	if err != nil {
		return EvalResult{}, err
	}
	return expr.eval(env)
}

func (env *ExpressionEnv) TypeOf(expr Expr) (querypb.Type, error) {
	return expr.typeof(env)
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return EnvWithBindVars(map[string]*querypb.BindVariable{})
}

// EnvWithBindVars returns an expression environment with no current row, but with bindvars
func EnvWithBindVars(bindVars map[string]*querypb.BindVariable) *ExpressionEnv {
	return &ExpressionEnv{BindVars: bindVars}
}

// NullExpr is just what you are lead to believe
var NullExpr = &Literal{Val: resultNull}

// NewLiteralIntegralFromBytes returns a literal expression.
// It tries to return an int64, but if the value is too large, it tries with an uint64
func NewLiteralIntegralFromBytes(val []byte) (Expr, error) {
	str := string(val)
	ival, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		return NewLiteralInt(ival), nil
	}

	// let's try with uint if we overflowed
	numError, ok := err.(*strconv.NumError)
	if !ok || numError.Err != strconv.ErrRange {
		return nil, err
	}

	uval, err := strconv.ParseUint(str, 0, 64)
	if err != nil {
		return nil, err
	}
	return NewLiteralUint(uval), nil
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) Expr {
	return &Literal{Val: newEvalInt64(i)}
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) Expr {
	return &Literal{Val: newEvalUint64(i)}
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	return &Literal{Val: newEvalFloat(val)}
}

// NewLiteralRealFromBytes returns a float literal expression from a slice of bytes
func NewLiteralRealFromBytes(val []byte) (Expr, error) {
	if bytes.IndexByte(val, 'e') >= 0 || bytes.IndexByte(val, 'E') >= 0 {
		fval, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return nil, err
		}
		return &Literal{Val: newEvalFloat(fval)}, nil
	}
	dec, err := newDecimalString(string(val))
	if err != nil {
		return nil, err
	}
	return &Literal{Val: newEvalDecimal(dec)}, nil
}

// NewLiteralString returns a literal expression
func NewLiteralString(val []byte, collation collations.TypedCollation) Expr {
	collation.Repertoire = collations.RepertoireASCII
	for _, b := range val {
		if b >= utf8.RuneSelf {
			collation.Repertoire = collations.RepertoireUnicode
			break
		}
	}
	return &Literal{Val: EvalResult{typ: sqltypes.VarBinary, bytes: val, collation: collation}}
}

// NewBindVar returns a bind variable
func NewBindVar(key string, collation collations.TypedCollation) Expr {
	return &BindVariable{
		Key:  key,
		coll: collation,
	}
}

// NewColumn returns a column expression
func NewColumn(offset int, collation collations.TypedCollation) Expr {
	return &Column{
		Offset: offset,
		coll:   collation,
	}
}

// NewTupleExpr returns a tuple expression
func NewTupleExpr(exprs ...Expr) TupleExpr {
	tupleExpr := make(TupleExpr, 0, len(exprs))
	for _, f := range exprs {
		tupleExpr = append(tupleExpr, f)
	}
	return tupleExpr
}

func (c *UnaryExpr) typeof(env *ExpressionEnv) (querypb.Type, error) {
	return c.Inner.typeof(env)
}

// eval implements the Expr interface
func (l *Literal) eval(*ExpressionEnv) (EvalResult, error) {
	return l.Val, nil
}

func (t TupleExpr) eval(env *ExpressionEnv) (EvalResult, error) {
	var tup []EvalResult
	for _, expr := range t {
		evalRes, err := expr.eval(env)
		if err != nil {
			return EvalResult{}, err
		}
		tup = append(tup, evalRes)
	}
	return EvalResult{
		typ:   querypb.Type_TUPLE,
		tuple: &tup,
	}, nil
}

// eval implements the Expr interface
func (bv *BindVariable) eval(env *ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[bv.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	eval, err := evaluateByType(val)
	if err != nil {
		return EvalResult{}, err
	}
	eval.collation = bv.coll
	return eval, nil
}

// eval implements the Expr interface
func (c *Column) eval(env *ExpressionEnv) (EvalResult, error) {
	value := env.Row[c.Offset]
	numeric, err := newEvalResult(value)
	numeric.collation = c.coll
	return numeric, err
}

// Type implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv) (querypb.Type, error) {
	e := env.BindVars
	v, found := e[bv.Key]
	if !found {
		return querypb.Type_NULL_TYPE, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", bv.Key)
	}
	return v.Type, nil
}

// Type implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv) (querypb.Type, error) {
	return l.Val.typ, nil
}

// Type implements the Expr interface
func (t TupleExpr) typeof(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_TUPLE, nil
}

func (c *Column) typeof(*ExpressionEnv) (querypb.Type, error) {
	return sqltypes.Float64, nil
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

func evaluateByTypeSingle(typ querypb.Type, value []byte) (EvalResult, error) {
	switch typ {
	case sqltypes.Int64:
		ival, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int64, numval: uint64(ival)}, nil
	case sqltypes.Int32:
		ival, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int32, numval: uint64(ival)}, nil
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			uval = 0
		}
		return EvalResult{typ: sqltypes.Uint64, numval: uval}, nil
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			fval = 0
		}
		return EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(fval)}, nil
	case sqltypes.Decimal:
		dec, err := newDecimalString(string(value))
		if err != nil {
			return EvalResult{}, err
		}
		return newEvalDecimal(dec), nil
	case sqltypes.VarChar, sqltypes.Text, sqltypes.VarBinary:
		return EvalResult{typ: sqltypes.VarBinary, bytes: value}, nil
	case sqltypes.Time, sqltypes.Datetime, sqltypes.Timestamp, sqltypes.Date:
		return EvalResult{typ: typ, bytes: value}, nil
	case sqltypes.Null:
		return resultNull, nil
	default:
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Type is not supported: %s", typ.String())
	}

}
func evaluateByType(val *querypb.BindVariable) (EvalResult, error) {
	switch val.Type {
	case querypb.Type_TUPLE:
		tuple := make([]EvalResult, 0, len(val.Values))
		for _, value := range val.Values {
			single, err := evaluateByTypeSingle(value.Type, value.Value)
			if err != nil {
				return EvalResult{}, err
			}
			tuple = append(tuple, single)
		}
		return EvalResult{
			typ:   querypb.Type_TUPLE,
			tuple: &tuple,
		}, nil

	default:
		return evaluateByTypeSingle(val.Type, val.Value)
	}
}
