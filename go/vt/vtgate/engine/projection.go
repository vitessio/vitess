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

// RouteType implements the Primitive interface
func (p *Projection) RouteType() string {
	return p.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (p *Projection) GetKeyspaceName() string {
	return p.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (p *Projection) GetTableName() string {
	return p.Input.GetTableName()
}

// TryExecute implements the Primitive interface
func (p *Projection) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(ctx, p.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var resultRows []sqltypes.Row
	for _, row := range result.Rows {
		resultRow := make(sqltypes.Row, 0, len(p.Exprs))
		env.Row = row
		for _, exp := range p.Exprs {
			c, err := env.Evaluate(exp)
			if err != nil {
				return nil, err
			}
			resultRow = append(resultRow, c.Value(vcursor.ConnCollation()))
		}
		resultRows = append(resultRows, resultRow)
	}
	if wantfields {
		result.Fields, err = p.evalFields(env, result.Fields)
		if err != nil {
			return nil, err
		}
	}
	result.Rows = resultRows
	return result, nil
}

// TryStreamExecute implements the Primitive interface
func (p *Projection) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var once sync.Once
	var fields []*querypb.Field
	var mu sync.Mutex
	return vcursor.StreamExecutePrimitive(ctx, p.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		var err error
		mu.Lock()
		defer mu.Unlock()
		if wantfields {
			once.Do(func() {
				fields, err = p.evalFields(env, qr.Fields)
				if err != nil {
					return
				}
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
		resultRows := make([]sqltypes.Row, 0, len(qr.Rows))
		for _, r := range qr.Rows {
			resultRow := make(sqltypes.Row, 0, len(p.Exprs))
			env.Row = r
			for _, exp := range p.Exprs {
				c, err := env.Evaluate(exp)
				if err != nil {
					return err
				}
				resultRow = append(resultRow, c.Value(vcursor.ConnCollation()))
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
	qr.Fields, err = p.evalFields(env, qr.Fields)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (p *Projection) evalFields(env *evalengine.ExpressionEnv, infields []*querypb.Field) ([]*querypb.Field, error) {
	// TODO: once the evalengine becomes smart enough, we should be able to remove the
	// dependency on these fields altogether
	env.Fields = infields

	var fields []*querypb.Field
	for i, col := range p.Cols {
		typ, err := env.TypeOf(p.Exprs[i])
		if err != nil {
			return nil, err
		}
		fl := mysql.FlagsForColumn(typ.Type(), typ.Collation())
		if !sqltypes.IsNull(typ.Type()) && !typ.Nullable() {
			fl |= uint32(querypb.MySqlFlag_NOT_NULL_FLAG)
		}
		fields = append(fields, &querypb.Field{
			Name:         col,
			Type:         typ.Type(),
			Charset:      uint32(typ.Collation()),
			ColumnLength: uint32(typ.Size()),
			Decimals:     uint32(typ.Scale()),
			Flags:        fl,
		})
	}
	return fields, nil
}

// Inputs implements the Primitive interface
func (p *Projection) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{p.Input}, nil
}

// description implements the Primitive interface
func (p *Projection) description() PrimitiveDescription {
	var exprs []string
	for idx, e := range p.Exprs {
		expr := sqlparser.String(e)
		alias := p.Cols[idx]
		if alias != "" {
			expr += " as " + alias
		}
		exprs = append(exprs, expr)
	}
	return PrimitiveDescription{
		OperatorType: "Projection",
		Other: map[string]any{
			"Expressions": exprs,
		},
	}
}
