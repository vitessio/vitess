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
		eval(env *ExpressionEnv, result *EvalResult)
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

type evalError struct {
	error
}

func throwEvalError(err error) {
	panic(evalError{err})
}

func (env *ExpressionEnv) Evaluate(expr Expr) (er EvalResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.error
			} else {
				panic(r)
			}
		}
	}()
	_, err = expr.cardinality(env)
	if err != nil {
		return EvalResult{}, err
	}
	expr.eval(env, &er)
	return
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
var NullExpr = &Literal{}

func init() {
	NullExpr.Val.setNull()
}

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
	lit := &Literal{}
	lit.Val.setInt64(i)
	return lit
}

// NewLiteralUint returns a literal expression
func NewLiteralUint(i uint64) Expr {
	lit := &Literal{}
	lit.Val.setUint64(i)
	return lit
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	lit := &Literal{}
	lit.Val.setFloat(val)
	return lit
}

// NewLiteralRealFromBytes returns a float literal expression from a slice of bytes
func NewLiteralRealFromBytes(val []byte) (Expr, error) {
	lit := &Literal{}
	if bytes.IndexByte(val, 'e') >= 0 || bytes.IndexByte(val, 'E') >= 0 {
		fval, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return nil, err
		}
		lit.Val.setFloat(fval)
		return lit, nil
	}
	dec, err := newDecimalString(string(val))
	if err != nil {
		return nil, err
	}
	lit.Val.setDecimal(dec)
	return lit, nil
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
	lit := &Literal{}
	lit.Val.setRaw(sqltypes.VarBinary, val, collation)
	return lit
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
func (l *Literal) eval(_ *ExpressionEnv, result *EvalResult) {
	*result = l.Val
}

func (t TupleExpr) eval(env *ExpressionEnv, result *EvalResult) {
	var tup = make([]EvalResult, len(t))
	for i, expr := range t {
		tup[i].init(env, expr)
	}
	result.setTuple(tup)
}

// eval implements the Expr interface
func (bv *BindVariable) eval(env *ExpressionEnv, result *EvalResult) {
	val, ok := env.BindVars[bv.Key]
	if !ok {
		throwEvalError(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found"))
	}
	result.setBindVar(val, bv.coll)
}

// eval implements the Expr interface
func (c *Column) eval(env *ExpressionEnv, result *EvalResult) {
	value := env.Row[c.Offset]
	if err := result.setValue(value); err != nil {
		throwEvalError(err)
	}
	result.replaceCollation(c.coll)
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv) (querypb.Type, error) {
	e := env.BindVars
	v, found := e[bv.Key]
	if !found {
		return querypb.Type_NULL_TYPE, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", bv.Key)
	}
	return v.Type, nil
}

// typeof implements the Expr interface
func (l *Literal) typeof(*ExpressionEnv) (querypb.Type, error) {
	return l.Val.typeof(), nil
}

// typeof implements the Expr interface
func (t TupleExpr) typeof(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_TUPLE, nil
}

func (c *Column) typeof(env *ExpressionEnv) (querypb.Type, error) {
	value := env.Row[c.Offset]
	return value.Type(), nil
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
