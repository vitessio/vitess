package engine

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*UpdateTarget)(nil)

// UpdateTarget is an operator to update target string.
type UpdateTarget struct {
	// Target string to be updated
	Target string

	noInputs

	noTxNeeded
}

func (updTarget *UpdateTarget) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "UpdateTarget",
		Other:        map[string]interface{}{"target": updTarget.Target},
	}
}

// RouteType implements the Primitive interface
func (updTarget *UpdateTarget) RouteType() string {
	return "UpdateTarget"
}

// GetKeyspaceName implements the Primitive interface
func (updTarget *UpdateTarget) GetKeyspaceName() string {
	return updTarget.Target
}

// GetTableName implements the Primitive interface
func (updTarget *UpdateTarget) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (updTarget *UpdateTarget) TryExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := vcursor.Session().SetTarget(updTarget.Target)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface
func (updTarget *UpdateTarget) TryStreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result, err := updTarget.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(result)
}

// GetFields implements the Primitive interface
func (updTarget *UpdateTarget) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields not reachable for use statement")
}
