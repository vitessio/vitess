package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*Projection)(nil)

type Projection struct {
	Exprs []*sqlparser.AliasedExpr
	Input Primitive
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

func (p *Projection) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

func (p *Projection) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (p *Projection) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (p *Projection) Inputs() []Primitive {
	return []Primitive{p.Input}
}

func (p *Projection) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Projection",
		Other: map[string]interface{}{
			"Expressions": p.Exprs,
		},
	}
}
