package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Projection)(nil)

type Projection struct {
	Cols  []string
	Exprs []sqltypes.Expr
	Input Primitive
	noTxNeeded
}

func (p *Projection) RouteType() string {
	return p.Input.RouteType()
}

func (p *Projection) GetKeyspaceName() string {
	return p.Input.GetKeyspaceName()
}

func (p *Projection) GetTableName() string {
	return p.Input.GetTableName()
}

func (p *Projection) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := p.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	env := sqltypes.ExpressionEnv{
		BindVars: bindVars,
	}

	if wantfields {
		p.addFields(result, bindVars)
	}
	var rows [][]sqltypes.Value
	for _, row := range result.Rows {
		env.Row = row
		for _, exp := range p.Exprs {
			result, err := exp.Evaluate(env)
			if err != nil {
				return nil, err
			}
			row = append(row, result.Value())
		}
		rows = append(rows, row)
	}
	result.Rows = rows
	return result, nil
}

func (p *Projection) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result, err := p.Input.Execute(vcursor, bindVars, wantields)
	if err != nil {
		return err
	}

	env := sqltypes.ExpressionEnv{
		BindVars: bindVars,
	}

	if wantields {
		p.addFields(result, bindVars)
	}
	var rows [][]sqltypes.Value
	for _, row := range result.Rows {
		env.Row = row
		for _, exp := range p.Exprs {
			result, err := exp.Evaluate(env)
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

func (p *Projection) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := p.Input.GetFields(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	p.addFields(qr, bindVars)
	return qr, nil
}

func (p *Projection) addFields(qr *sqltypes.Result, bindVars map[string]*querypb.BindVariable) {
	env := sqltypes.ExpressionEnv{BindVars: bindVars}
	for i, col := range p.Cols {
		qr.Fields = append(qr.Fields, &querypb.Field{
			Name: col,
			Type: p.Exprs[i].Type(env),
		})
	}
}

func (p *Projection) Inputs() []Primitive {
	return []Primitive{p.Input}
}

func (p *Projection) description() PrimitiveDescription {
	var exprs []string
	for _, e := range p.Exprs {
		exprs = append(exprs, e.String())
	}
	return PrimitiveDescription{
		OperatorType: "Projection",
		Other: map[string]interface{}{
			"Expressions": exprs,
			"Columns":     p.Cols,
		},
	}
}
