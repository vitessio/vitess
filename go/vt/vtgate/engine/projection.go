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
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*Projection)(nil)

// Projection can evaluate expressions and project the results
type Projection struct {
	Cols  []string
	Exprs []evalengine.Expr
	Input Primitive
	noTxNeeded
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
<<<<<<< HEAD
		result.Fields, err = p.evalFields(env, result.Fields, vcursor)
=======
		result.Fields, err = p.evalFields(env, result.Fields, vcursor.ConnCollation())
>>>>>>> 5fd70c4323 (projection: Return correct collation information (#15801))
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
<<<<<<< HEAD
				fields, err = p.evalFields(env, qr.Fields, vcursor)
=======
				fields, err = p.evalFields(env, qr.Fields, vcursor.ConnCollation())
>>>>>>> 5fd70c4323 (projection: Return correct collation information (#15801))
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
<<<<<<< HEAD
	qr.Fields, err = p.evalFields(env, qr.Fields, vcursor)
=======
	qr.Fields, err = p.evalFields(env, qr.Fields, vcursor.ConnCollation())
>>>>>>> 5fd70c4323 (projection: Return correct collation information (#15801))
	if err != nil {
		return nil, err
	}
	return qr, nil
}

<<<<<<< HEAD
func (p *Projection) evalFields(env *evalengine.ExpressionEnv, infields []*querypb.Field, vcursor VCursor) ([]*querypb.Field, error) {
=======
func (p *Projection) evalFields(env *evalengine.ExpressionEnv, infields []*querypb.Field, coll collations.ID) ([]*querypb.Field, error) {
	// TODO: once the evalengine becomes smart enough, we should be able to remove the
	// dependency on these fields altogether
	env.Fields = infields

>>>>>>> 5fd70c4323 (projection: Return correct collation information (#15801))
	var fields []*querypb.Field
	for i, col := range p.Cols {
		q, f, err := env.TypeOf(p.Exprs[i], infields)
		if err != nil {
			return nil, err
		}
		var cs collations.ID = collations.CollationBinaryID
		if sqltypes.IsText(q) {
			cs = vcursor.ConnCollation()
		}

		fl := mysql.FlagsForColumn(q, cs)
		if !sqltypes.IsNull(q) && !f.Nullable() {
			fl |= uint32(querypb.MySqlFlag_NOT_NULL_FLAG)
		}
		typCol := typ.Collation()
		if sqltypes.IsTextOrBinary(typ.Type()) && typCol != collations.CollationBinaryID {
			typCol = coll
		}

		fields = append(fields, &querypb.Field{
<<<<<<< HEAD
			Name:    col,
			Type:    q,
			Charset: uint32(cs),
			Flags:   fl,
=======
			Name:         col,
			Type:         typ.Type(),
			Charset:      uint32(typCol),
			ColumnLength: uint32(typ.Size()),
			Decimals:     uint32(typ.Scale()),
			Flags:        fl,
>>>>>>> 5fd70c4323 (projection: Return correct collation information (#15801))
		})
	}
	return fields, nil
}

// Inputs implements the Primitive interface
func (p *Projection) Inputs() []Primitive {
	return []Primitive{p.Input}
}

// description implements the Primitive interface
func (p *Projection) description() PrimitiveDescription {
	var exprs []string
	for idx, e := range p.Exprs {
		expr := evalengine.FormatExpr(e)
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
