package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*AlterVSchema)(nil)

//AlterVSchema operator applies changes to VSchema
type AlterVSchema struct {
	Keyspace *vindexes.Keyspace

	AlterVschemaDDL *sqlparser.AlterVschema

	noTxNeeded

	noInputs
}

func (v *AlterVSchema) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "AlterVSchema",
		Keyspace:     v.Keyspace,
		Other: map[string]interface{}{
			"query": sqlparser.String(v.AlterVschemaDDL),
		},
	}
}

// RouteType implements the Primitive interface
func (v *AlterVSchema) RouteType() string {
	return "AlterVSchema"
}

// GetKeyspaceName implements the Primitive interface
func (v *AlterVSchema) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *AlterVSchema) GetTableName() string {
	return v.AlterVschemaDDL.Table.Name.String()
}

// TryExecute implements the Primitive interface
func (v *AlterVSchema) TryExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	err := vcursor.ExecuteVSchema(v.Keyspace.Name, v.AlterVschemaDDL)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface
func (v *AlterVSchema) TryStreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := v.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields implements the Primitive interface
func (v *AlterVSchema) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.NewErrorf(vtrpcpb.Code_UNIMPLEMENTED, vterrors.UnsupportedPS, "This command is not supported in the prepared statement protocol yet")
}
