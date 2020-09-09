package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*SQLCalFoundRows)(nil)

//SQLCalFoundRows is a primitive to execute limit and count query as per their individual plan.
type SQLCalFoundRows struct {
	LimitPrimitive Primitive
	CountPrimitive Primitive
}

//RouteType implements the Primitive interface
func (s SQLCalFoundRows) RouteType() string {
	return "SQLCalcFoundRows"
}

//GetKeyspaceName implements the Primitive interface
func (s SQLCalFoundRows) GetKeyspaceName() string {
	return s.LimitPrimitive.GetKeyspaceName()
}

//GetTableName implements the Primitive interface
func (s SQLCalFoundRows) GetTableName() string {
	return s.LimitPrimitive.GetTableName()
}

//Execute implements the Primitive interface
func (s SQLCalFoundRows) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	limitQr, err := s.LimitPrimitive.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	countQr, err := s.CountPrimitive.Execute(vcursor, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(countQr.Rows) != 1 || len(countQr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "count query is not a scalar")
	}
	fr, err := evalengine.ToUint64(countQr.Rows[0][0])
	if err != nil {
		return nil, err
	}
	vcursor.Session().SetFoundRows(fr)
	return limitQr, nil
}

//StreamExecute implements the Primitive interface
func (s SQLCalFoundRows) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

//GetFields implements the Primitive interface
func (s SQLCalFoundRows) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return s.LimitPrimitive.GetFields(vcursor, bindVars)
}

//NeedsTransaction implements the Primitive interface
func (s SQLCalFoundRows) NeedsTransaction() bool {
	return s.LimitPrimitive.NeedsTransaction()
}

//Inputs implements the Primitive interface
func (s SQLCalFoundRows) Inputs() []Primitive {
	return []Primitive{s.LimitPrimitive, s.CountPrimitive}
}

func (s SQLCalFoundRows) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SQL_CALC_FOUND_ROWS",
	}
}
