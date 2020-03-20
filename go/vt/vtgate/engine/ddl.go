package engine

import (
	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*DDL)(nil)

//DDL is primitive
type DDL struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace string

	// TargetDestination specifies the destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	//DDL does not need inputs
	noInputs

	//DDL does not execute inside a transaction
	noTxNeeded
}

// MarshalJSON serializes the DDL into a JSON representation.
// It's used for testing and diagnostics.
func (ddl *DDL) MarshalJSON() ([]byte, error) {
	marshalDDL := struct {
		Type              string
		Keyspace          string `json:",omitempty"`
		TargetDestination string `json:",omitempty"`
		Query             string `json:",omitempty"`
	}{
		Type:              "DDL",
		Keyspace:          ddl.Keyspace,
		Query:             ddl.Query,
		TargetDestination: ddl.TargetDestination.String(),
	}
	return jsonutil.MarshalNoEscape(marshalDDL)
}

// RouteType implements the primitive interface
func (ddl *DDL) RouteType() string {
	return "DDL"
}

// GetKeyspaceName implements the primitive interface
func (ddl *DDL) GetKeyspaceName() string {
	return ddl.Keyspace
}

// GetTableName implements the primitive interface
func (ddl *DDL) GetTableName() string {
	return ""
}

// Execute  implements the primitive interface
func (ddl *DDL) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	//TODO(harshit): Add a method in vcursor to provide destination if target is set.
	rss, _, err := vcursor.ResolveDestinations(ddl.Keyspace, nil, []key.Destination{ddl.TargetDestination})
	if err != nil {
		return nil, vterrors.Wrap(err, "ddlExecute")
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           ddl.Query,
			BindVariables: bindVars,
		}
	}

	result, errs := vcursor.ExecuteMultiShard(rss, queries, false, true)
	err = vterrors.Aggregate(errs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

//StreamExecute implements the primitive interface
func (ddl *DDL) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

//GetFields implements the primitive interface
func (ddl *DDL) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}
