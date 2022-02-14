package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SingleRow)(nil)

// SingleRow defines an empty result
type SingleRow struct {
	noInputs
	noTxNeeded
}

// RouteType returns a description of the query routing type used by the primitive
func (s *SingleRow) RouteType() string {
	return ""
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (s *SingleRow) GetKeyspaceName() string {
	return ""
}

// GetTableName specifies the table that this primitive routes to.
func (s *SingleRow) GetTableName() string {
	return ""
}

// TryExecute performs a non-streaming exec.
func (s *SingleRow) TryExecute(VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	result := sqltypes.Result{
		Rows: [][]sqltypes.Value{
			{},
		},
	}
	return &result, nil
}

// TryStreamExecute performs a streaming exec.
func (s *SingleRow) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := s.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (s *SingleRow) GetFields(_ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (s *SingleRow) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SingleRow",
	}
}
