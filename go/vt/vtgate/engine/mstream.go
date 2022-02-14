package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*MStream)(nil)

// MStream is an operator for message streaming from specific keyspace, destination
type MStream struct {
	// Keyspace specifies the keyspace to stream messages from
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to stream messages from
	TargetDestination key.Destination

	// TableName specifies the table on which stream will be executed.
	TableName string

	noTxNeeded

	noInputs
}

// RouteType implements the Primitive interface
func (m *MStream) RouteType() string {
	return "MStream"
}

// GetKeyspaceName implements the Primitive interface
func (m *MStream) GetKeyspaceName() string {
	return m.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (m *MStream) GetTableName() string {
	return m.TableName
}

// TryExecute implements the Primitive interface
func (m *MStream) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] 'Execute' called for Stream")
}

// TryStreamExecute implements the Primitive interface
func (m *MStream) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	rss, _, err := vcursor.ResolveDestinations(m.Keyspace.Name, nil, []key.Destination{m.TargetDestination})
	if err != nil {
		return err
	}
	return vcursor.MessageStream(rss, m.TableName, callback)
}

// GetFields implements the Primitive interface
func (m *MStream) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] 'GetFields' called for Stream")
}

func (m *MStream) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "MStream",
		Keyspace:          m.Keyspace,
		TargetDestination: m.TargetDestination,

		Other: map[string]interface{}{"Table": m.TableName},
	}
}
