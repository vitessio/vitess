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
func (p *Projection) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := vcursor.ExecutePrimitive(p.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	env.Fields = result.Fields
	var resultRows []sqltypes.Row
	for _, row := range result.Rows {
		resultRow := make(sqltypes.Row, 0, len(p.Exprs))
		env.Row = row
		for _, exp := range p.Exprs {
			result, err := env.Evaluate(exp)
			if err != nil {
				return nil, err
			}
			resultRow = append(resultRow, result.Value())
		}
		resultRows = append(resultRows, resultRow)
	}
	if wantfields {
		err := p.addFields(env, result)
		if err != nil {
			return nil, err
		}
	}
	result.Rows = resultRows
	return result, nil
}

// TryStreamExecute implements the Primitive interface
func (p *Projection) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result, err := vcursor.ExecutePrimitive(p.Input, bindVars, wantields)
	if err != nil {
		return err
	}

	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	if wantields {
		err = p.addFields(env, result)
		if err != nil {
			return err
		}
	}
	var rows [][]sqltypes.Value
	env.Fields = result.Fields
	for _, row := range result.Rows {
		env.Row = row
		for _, exp := range p.Exprs {
			result, err := env.Evaluate(exp)
			if err != nil {
				return err
			}
			row = append(row, result.Value())
		}
		rows = append(rows, row)
	}
	result.Rows = rows
	return callback(result)
}

// GetFields implements the Primitive interface
func (p *Projection) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := p.Input.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	err = p.addFields(env, qr)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

func (p *Projection) addFields(env *evalengine.ExpressionEnv, qr *sqltypes.Result) error {
	qr.Fields = nil
	for i, col := range p.Cols {
		q, err := env.TypeOf(p.Exprs[i])
		if err != nil {
			return err
		}
		qr.Fields = append(qr.Fields, &querypb.Field{
			Name: col,
			Type: q,
		})
	}
	return nil
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
		Other: map[string]interface{}{
			"Expressions": exprs,
		},
	}
}
