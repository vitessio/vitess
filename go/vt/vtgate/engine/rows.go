package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Rows)(nil)

// Rows simply returns a number or rows
type Rows struct {
	rows   [][]sqltypes.Value
	fields []*querypb.Field

	noInputs
	noTxNeeded
}

// NewRowsPrimitive returns a new Rows primitie
func NewRowsPrimitive(rows [][]sqltypes.Value, fields []*querypb.Field) *Rows {
	return &Rows{rows: rows, fields: fields}
}

// RouteType implements the Primitive interface
func (r *Rows) RouteType() string {
	return "Rows"
}

// GetKeyspaceName implements the Primitive interface
func (r *Rows) GetKeyspaceName() string {
	return ""
}

// GetTableName implements the Primitive interface
func (r *Rows) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (r *Rows) TryExecute(VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields:   r.fields,
		InsertID: 0,
		Rows:     r.rows,
	}, nil
}

// TryStreamExecute implements the Primitive interface
func (r *Rows) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result, err := r.TryExecute(vcursor, bindVars, wantields)
	if err != nil {
		return err
	}
	return callback(result)
}

// GetFields implements the Primitive interface
func (r *Rows) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{
		Fields:   r.fields,
		InsertID: 0,
		Rows:     nil,
	}, nil
}

func (r *Rows) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "Rows"}
}
