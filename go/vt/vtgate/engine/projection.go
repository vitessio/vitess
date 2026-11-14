/*
Copyright 2022 The Vitess Authors.

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

package engine

import (
	"context"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*Projection)(nil)

// Projection can evaluate expressions and project the results
type Projection struct {
	noTxNeeded

	Cols  []string
	Exprs []evalengine.Expr
	Input Primitive
}

// TryExecute implements the Primitive interface
func (p *Projection) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(ctx, p.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var types []evalengine.Type
	if wantfields || len(result.Rows) > 0 {
		types, err = p.exprTypes(env, result.Fields, result.Rows, wantfields)
		if err != nil {
			return nil, err
		}
	}
	stamp := stampTypes(types)
	var resultRows []sqltypes.Row
	for _, row := range result.Rows {
		resultRow := make(sqltypes.Row, 0, len(p.Exprs))
		env.Row = row
		for i, exp := range p.Exprs {
			c, err := env.Evaluate(exp)
			if err != nil {
				return nil, err
			}
			v := c.Value(vcursor.ConnCollation())
			if stamp != nil {
				v = evalengine.StampFieldType(v, stamp[i])
			}
			resultRow = append(resultRow, v)
		}
		resultRows = append(resultRows, resultRow)
	}
	if wantfields {
		result.Fields = p.fields(types, vcursor.ConnCollation())
	}
	result.Rows = resultRows
	return result, nil
}

// TryStreamExecute implements the Primitive interface
func (p *Projection) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var once sync.Once
	var fields []*querypb.Field
	var typed bool
	var stamp []sqltypes.Type
	var mu sync.Mutex
	return vcursor.StreamExecutePrimitive(ctx, p.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		var err error
		mu.Lock()
		defer mu.Unlock()
		if wantfields {
			once.Do(func() {
				var types []evalengine.Type
				types, err = p.exprTypes(env, qr.Fields, qr.Rows, true)
				if err != nil {
					return
				}
				typed, stamp = true, stampTypes(types)
				fields = p.fields(types, vcursor.ConnCollation())
				err = callback(&sqltypes.Result{Fields: fields})
				if err != nil {
					return
				}
			})
			qr.Fields = fields
		}
		if err != nil {
			return err
		}
		if !typed && len(qr.Rows) > 0 {
			types, _ := p.exprTypes(env, qr.Fields, qr.Rows, false)
			typed, stamp = true, stampTypes(types)
		}
		resultRows := make([]sqltypes.Row, 0, len(qr.Rows))
		for _, r := range qr.Rows {
			resultRow := make(sqltypes.Row, 0, len(p.Exprs))
			env.Row = r
			for i, exp := range p.Exprs {
				c, err := env.Evaluate(exp)
				if err != nil {
					return err
				}
				v := c.Value(vcursor.ConnCollation())
				if stamp != nil {
					v = evalengine.StampFieldType(v, stamp[i])
				}
				resultRow = append(resultRow, v)
			}
			resultRows = append(resultRows, resultRow)
		}
		qr.Rows = resultRows
		return callback(qr)
	})
}

// GetFields implements the Primitive interface
func (p *Projection) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := p.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	qr.Fields, err = p.evalFields(env, qr.Fields, vcursor.ConnCollation())
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (p *Projection) evalFields(env *evalengine.ExpressionEnv, infields []*querypb.Field, coll collations.ID) ([]*querypb.Field, error) {
	types, err := p.exprTypes(env, infields, nil, true)
	if err != nil {
		return nil, err
	}
	return p.fields(types, coll), nil
}

// exprTypes returns the type of each expression for one execution, from the
// fields of the input and its first row. The type of a string function
// whose length depends on bind variables is decided here: REPEAT(s, :n) is
// a VARCHAR for n = 2 and a TEXT for n = 1639. When the types are not
// needed for the fields, an error is not one of the query, and no types are
// returned.
func (p *Projection) exprTypes(env *evalengine.ExpressionEnv, infields []*querypb.Field, rows []sqltypes.Row, needed bool) ([]evalengine.Type, error) {
	// TODO: once the evalengine becomes smart enough, we should be able to remove the
	// dependency on these fields altogether
	env.Fields = infields
	if len(rows) > 0 {
		env.Row = rows[0]
	}

	types := make([]evalengine.Type, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		typ, err := env.TypeOf(expr)
		if err != nil {
			if needed {
				return nil, err
			}
			return nil, nil
		}
		types = append(types, typ)
	}
	return types, nil
}

// stampTypes returns the type of each field when a value must be given the
// type of its field, a TEXT or a BLOB, and nil when none must.
func stampTypes(types []evalengine.Type) []sqltypes.Type {
	var stamp []sqltypes.Type
	for i, typ := range types {
		if tt := typ.Type(); tt == sqltypes.Text || tt == sqltypes.Blob {
			if stamp == nil {
				stamp = make([]sqltypes.Type, len(types))
			}
			stamp[i] = tt
		}
	}
	return stamp
}

// fields returns the fields of the projection from the types of its
// expressions.
func (p *Projection) fields(types []evalengine.Type, coll collations.ID) []*querypb.Field {
	var fields []*querypb.Field
	for i, col := range p.Cols {
		typ := types[i]
		fl := mysql.FlagsForColumn(typ.Type(), typ.Collation())
		if !sqltypes.IsNull(typ.Type()) && !typ.Nullable() {
			fl |= uint32(querypb.MySqlFlag_NOT_NULL_FLAG)
		}
		typCol := typ.Collation()
		if sqltypes.IsTextOrBinary(typ.Type()) && typCol != collations.CollationBinaryID {
			typCol = coll
		}

		fields = append(fields, &querypb.Field{
			Name:         col,
			Type:         typ.Type(),
			Charset:      uint32(typCol),
			ColumnLength: uint32(typ.Size()),
			Decimals:     uint32(typ.Scale()),
			Flags:        fl,
		})
	}
	return fields
}

// Inputs implements the Primitive interface
func (p *Projection) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{p.Input}, nil
}

// description implements the Primitive interface
func (p *Projection) description() PrimitiveDescription {
	var exprs []string
	if len(p.Exprs) > 0 {
		exprs = make([]string, 0, len(p.Exprs))
		for idx, e := range p.Exprs {
			expr := sqlparser.String(e)
			alias := p.Cols[idx]
			if alias != "" {
				expr += " as " + alias
			}
			exprs = append(exprs, expr)
		}
	}
	return PrimitiveDescription{
		OperatorType: "Projection",
		Other: map[string]any{
			"Expressions": exprs,
		},
	}
}
