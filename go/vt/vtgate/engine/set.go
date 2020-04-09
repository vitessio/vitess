package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type (
	// Set contains the instructions to perform set.
	Set struct {
		Ops []SetOp
		noTxNeeded
		noInputs
	}

	// SetOp is an interface that different type of set operations implements.
	SetOp interface {
		Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) error
		VariableName() string
	}

	// UserDefinedVariable implements the SetOp interface to execute user defined variables.
	UserDefinedVariable struct {
		Name      string
		PlanValue sqltypes.PlanValue
	}
)

var _ Primitive = (*Set)(nil)

//RouteType implements the Primitive interface method.
func (s *Set) RouteType() string {
	return "Set"
}

//GetKeyspaceName implements the Primitive interface method.
func (s *Set) GetKeyspaceName() string {
	return ""
}

//GetTableName implements the Primitive interface method.
func (s *Set) GetTableName() string {
	return ""
}

//Execute implements the Primitive interface method.
func (s *Set) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for _, setOp := range s.Ops {
		err := setOp.Execute(vcursor, bindVars)
		if err != nil {
			return nil, err
		}
	}
	return &sqltypes.Result{}, nil
}

//StreamExecute implements the Primitive interface method.
func (s *Set) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

//GetFields implements the Primitive interface method.
func (s *Set) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (s *Set) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Ops": s.Ops,
	}
	return PrimitiveDescription{
		OperatorType: "Set",
		Variant:      "",
		Other:        other,
	}
}

var _ SetOp = (*UserDefinedVariable)(nil)

//VariableName implements the SetOp interface method.
func (u *UserDefinedVariable) VariableName() string {
	return u.Name
}

//Execute implements the SetOp interface method.
func (u *UserDefinedVariable) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) error {
	value, err := u.PlanValue.ResolveValue(bindVars)
	if err != nil {
		return err
	}
	return vcursor.SetUDV(u.Name, value)
}
