package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*ReplaceVariables)(nil)

// ReplaceVariables is used in SHOW VARIABLES statements so that it replaces the values for vitess-aware variables
type ReplaceVariables struct {
	Input Primitive
	noTxNeeded
}

// NewReplaceVariables is used to create a new ReplaceVariables primitive
func NewReplaceVariables(input Primitive) *ReplaceVariables {
	return &ReplaceVariables{Input: input}
}

// RouteType implements the Primitive interface
func (r *ReplaceVariables) RouteType() string {
	return r.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (r *ReplaceVariables) GetKeyspaceName() string {
	return r.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (r *ReplaceVariables) GetTableName() string {
	return r.Input.GetTableName()
}

// TryExecute implements the Primitive interface
func (r *ReplaceVariables) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(r.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	replaceVariables(qr, bindVars)
	return qr, nil
}

// TryStreamExecute implements the Primitive interface
func (r *ReplaceVariables) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	innerCallback := callback
	callback = func(result *sqltypes.Result) error {
		replaceVariables(result, bindVars)
		return innerCallback(result)
	}
	return vcursor.StreamExecutePrimitive(r.Input, bindVars, wantfields, callback)
}

// GetFields implements the Primitive interface
func (r *ReplaceVariables) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Input.GetFields(vcursor, bindVars)
}

// Inputs implements the Primitive interface
func (r *ReplaceVariables) Inputs() []Primitive {
	return []Primitive{r.Input}
}

// description implements the Primitive interface
func (r *ReplaceVariables) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ReplaceVariables",
	}
}

func replaceVariables(qr *sqltypes.Result, bindVars map[string]*querypb.BindVariable) {
	for i, row := range qr.Rows {
		variableName := row[0].ToString()
		res, found := bindVars["__vt"+variableName]
		if found {
			qr.Rows[i][1] = sqltypes.NewVarChar(string(res.GetValue()))
		}
	}
}
