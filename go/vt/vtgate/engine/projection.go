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
	var rows [][]sqltypes.Value
	for _, row := range result.Rows {
		env.Row = row
		resRow := make([]sqltypes.Value, 0, len(p.Exprs))
		for _, exp := range p.Exprs {
			result, err := env.Evaluate(exp)
			if err != nil {
				return nil, err
			}
			resRow = append(resRow, result.Value())
		}
		rows = append(rows, resRow)
	}
	if wantfields {
		err := p.addFields(env, result)
		if err != nil {
			return nil, err
		}
	}
	result.Rows = rows
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
	for _, e := range p.Exprs {
		exprs = append(exprs, evalengine.FormatExpr(e))
	}
	return PrimitiveDescription{
		OperatorType: "Projection",
		Other: map[string]interface{}{
			"Expressions": exprs,
			"Columns":     p.Cols,
		},
	}
}
