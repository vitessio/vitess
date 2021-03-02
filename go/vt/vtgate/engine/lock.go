/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

// Execute is part of the Primitive interface
func (l *Lock) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(l.Keyspace.Name, nil, []key.Destination{l.TargetDestination})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "lock query can be routed to single shard only: %v", rss)
	}

	query := &querypb.BoundQuery{
		Sql:           l.Query,
		BindVariables: bindVars,
	}
	return vcursor.ExecuteLock(rss[0], query)
}

// StreamExecute is part of the Primitive interface
func (l *Lock) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := l.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

// GetFields is part of the Primitive interface
func (l *Lock) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "not implements in lock primitive")
}

func (l *Lock) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Query": l.Query,
	}
	return PrimitiveDescription{
		OperatorType:      "Lock",
		Keyspace:          l.Keyspace,
		TargetDestination: l.TargetDestination,
		Other:             other,
	}
}
