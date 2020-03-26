package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*Send)(nil)

// Send is an operator to send query to the specific keyspace, tabletType and destination
type Send struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	// This bypases the core of the v3 engine.
	TargetDestination key.Destination

	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType

	// Query specifies the query to be executed.
	Query string
}

// RouteType implements Primitive interface
func (s Send) RouteType() string {
	panic("implement me")
}

// GetKeyspaceName implements Primitive interface
func (s Send) GetKeyspaceName() string {
	panic("implement me")
}

// GetTableName implements Primitive interface
func (s Send) GetTableName() string {
	panic("implement me")
}

// Execute implements Primitive interface
func (s Send) Execute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

// StreamExecute implements Primitive interface
func (s Send) StreamExecute(vcursor VCursor, bindVars map[string]*query.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields implements Primitive interface
func (s Send) GetFields(vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

// Inputs implements Primitive interface
func (s Send) Inputs() []Primitive {
	panic("implement me")
}
