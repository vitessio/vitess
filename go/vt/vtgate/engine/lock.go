package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*Lock)(nil)

//Lock primitive will execute sql containing lock functions
type Lock struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	FieldQuery string

	noInputs

	noTxNeeded
}

// RouteType is part of the Primitive interface
func (l *Lock) RouteType() string {
	return "lock"
}

// GetKeyspaceName is part of the Primitive interface
func (l *Lock) GetKeyspaceName() string {
	return l.Keyspace.Name
}

// GetTableName is part of the Primitive interface
func (l *Lock) GetTableName() string {
	return "dual"
}

// TryExecute is part of the Primitive interface
func (l *Lock) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	return l.execLock(vcursor, l.Query, bindVars)
}

func (l *Lock) execLock(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(l.Keyspace.Name, nil, []key.Destination{l.TargetDestination})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "lock query can be routed to single shard only: %v", rss)
	}

	boundQuery := &querypb.BoundQuery{
		Sql:           query,
		BindVariables: bindVars,
	}
	return vcursor.ExecuteLock(rss[0], boundQuery)
}

// TryStreamExecute is part of the Primitive interface
func (l *Lock) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := l.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

// GetFields is part of the Primitive interface
func (l *Lock) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return l.execLock(vcursor, l.FieldQuery, bindVars)
}

func (l *Lock) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Query":      l.Query,
		"FieldQuery": l.FieldQuery,
	}
	return PrimitiveDescription{
		OperatorType:      "Lock",
		Keyspace:          l.Keyspace,
		TargetDestination: l.TargetDestination,
		Other:             other,
	}
}
