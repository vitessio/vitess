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
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	EvalResult struct {
		typ       querypb.Type
		collation collations.TypedCollation
		numval    uint64
		bytes     []byte
		tuple     *[]EvalResult
	}

	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
	}

	// Expr is the interface that all evaluating expressions must implement
	Expr interface {
		Evaluate(env *ExpressionEnv) (EvalResult, error)
		Type(env *ExpressionEnv) (querypb.Type, error)
		Collation() collations.TypedCollation
		String() string
	}

	Literal struct {
		Val EvalResult
	}
	BindVariable struct {
		Key       string
		collation collations.TypedCollation
	}
	Column struct {
		Offset    int
		collation collations.TypedCollation
	}
	TupleExpr   []Expr
	CollateExpr struct {
		Expr           Expr
		TypedCollation collations.TypedCollation
	}
)

func (t TupleExpr) Collation() collations.TypedCollation {
	// a Tuple does not have a collation, but an individual collation for every element of the tuple
	return collations.TypedCollation{}
}

var _ Expr = (*Literal)(nil)
var _ Expr = (*BindVariable)(nil)
var _ Expr = (*Column)(nil)
var _ Expr = (*BinaryExpr)(nil)
var _ Expr = (*ComparisonExpr)(nil)
var _ Expr = (TupleExpr)(nil)
var _ Expr = (*CollateExpr)(nil)

// Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ)
}

var collationNull = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceIgnorable,
	Repertoire:   collations.RepertoireASCII,
}

func NewLiteralNull() Expr {
	return &Literal{Val: EvalResult{typ: querypb.Type_NULL_TYPE, collation: collationNull}}
}

// NewLiteralIntFromBytes returns a literal expression
func NewLiteralIntFromBytes(val []byte) (Expr, error) {
	ival, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return nil, err
	}
	return NewLiteralInt(ival), nil
}

var collationNumeric = collations.TypedCollation{
	Collation:    collations.CollationBinaryID,
	Coercibility: collations.CoerceNumeric,
	Repertoire:   collations.RepertoireASCII,
}

// NewLiteralInt returns a literal expression
func NewLiteralInt(i int64) Expr {
	return &Literal{Val: EvalResult{typ: sqltypes.Int64, numval: uint64(i), collation: collationNumeric}}
}

// NewLiteralFloat returns a literal expression
func NewLiteralFloat(val float64) Expr {
	return &Literal{Val: EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(val), collation: collationNumeric}}
}

// NewLiteralFloatFromBytes returns a float literal expression from a slice of bytes
func NewLiteralFloatFromBytes(val []byte) (Expr, error) {
	fval, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return nil, err
	}
	return &Literal{Val: EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(fval)}}, nil
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
		Key:       key,
		collation: collation,
	}
}

// NewColumn returns a bind variable
func NewColumn(offset int, collation collations.TypedCollation) Expr {
	return &Column{
		Offset:    offset,
		collation: collation,
	}
}

// Evaluate implements the Expr interface
func (l *Literal) Evaluate(*ExpressionEnv) (EvalResult, error) {
	return l.Val, nil
}

func (l *Literal) Collation() collations.TypedCollation {
	return l.Val.collation
}

func (t TupleExpr) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	var tup []EvalResult
	for _, expr := range t {
		evalRes, err := expr.Evaluate(env)
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

// Evaluate implements the Expr interface
func (b *BindVariable) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	val, ok := env.BindVars[b.Key]
	if !ok {
		return EvalResult{}, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Bind variable not found")
	}
	eval, err := evaluateByType(val)
	if err != nil {
		return EvalResult{}, err
	}
	eval.collation = b.collation
	return eval, nil
}

func (b *BindVariable) Collation() collations.TypedCollation {
	return b.collation
}

// Evaluate implements the Expr interface
func (c *Column) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	value := env.Row[c.Offset]
	numeric, err := newEvalResult(value)
	numeric.collation = c.collation
	return numeric, err
}

func (c *Column) Collation() collations.TypedCollation {
	return c.collation
}

// Type implements the Expr interface
func (b *BindVariable) Type(env *ExpressionEnv) (querypb.Type, error) {
	e := env.BindVars
	v, found := e[b.Key]
	if !found {
		return querypb.Type_NULL_TYPE, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", b.Key)
	}
	return v.Type, nil
}

// Type implements the Expr interface
func (l *Literal) Type(*ExpressionEnv) (querypb.Type, error) {
	return l.Val.typ, nil
}

// Type implements the Expr interface
func (t TupleExpr) Type(*ExpressionEnv) (querypb.Type, error) {
	return querypb.Type_TUPLE, nil
}

func (c *Column) Type(*ExpressionEnv) (querypb.Type, error) {
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
func (t TupleExpr) String() string {
	var stringSlice []string
	for _, expr := range t {
		stringSlice = append(stringSlice, expr.String())
	}
	return "TUPLE(" + strings.Join(stringSlice, ", ") + ")"
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
		return EvalResult{typ: sqltypes.Int64, numval: uint64(ival)}, nil
	case sqltypes.Int32:
		ival, err := strconv.ParseInt(string(val.Value), 10, 32)
		if err != nil {
			ival = 0
		}
		return EvalResult{typ: sqltypes.Int32, numval: uint64(ival)}, nil
	case sqltypes.Uint64:
		uval, err := strconv.ParseUint(string(val.Value), 10, 64)
		if err != nil {
			uval = 0
		}
		return EvalResult{typ: sqltypes.Uint64, numval: uval}, nil
	case sqltypes.Float64:
		fval, err := strconv.ParseFloat(string(val.Value), 64)
		if err != nil {
			fval = 0
		}
		return EvalResult{typ: sqltypes.Float64, numval: math.Float64bits(fval)}, nil
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
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(e.typ)], e.numval, e.bytes)
}

func (c *CollateExpr) Evaluate(env *ExpressionEnv) (EvalResult, error) {
	res, err := c.Expr.Evaluate(env)
	if err != nil {
		return EvalResult{}, err
	}
	if err := collations.Local().EnsureCollate(res.collation.Collation, c.TypedCollation.Collation); err != nil {
		return EvalResult{}, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, err.Error())
	}
	res.collation = c.TypedCollation
	return res, nil
}

func (c *CollateExpr) Type(env *ExpressionEnv) (querypb.Type, error) {
	return c.Expr.Type(env)
}

func (c *CollateExpr) Collation() collations.TypedCollation {
	return c.TypedCollation
}

func (c *CollateExpr) String() string {
	coll := collations.Local().LookupByID(c.TypedCollation.Collation)
	return fmt.Sprintf("%s COLLATE %s", c.Expr.String(), coll.Name())
}
