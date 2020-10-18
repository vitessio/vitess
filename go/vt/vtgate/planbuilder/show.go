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

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildShowPlan(stmt *sqlparser.Show, vschema ContextVSchema) (engine.Primitive, error) {
	switch show := stmt.Internal.(type) {
	case *sqlparser.ShowColumns:
		return buildShowColumnsPlan(show, vschema)
	case *sqlparser.ShowTableStatus:
		return buildShowTableStatusPlan(show, vschema)
	default:
		return nil, ErrPlanNotSupported
	}
}

func buildShowColumnsPlan(show *sqlparser.ShowColumns, vschema ContextVSchema) (engine.Primitive, error) {
	if show.DbName != "" {
		show.Table.Qualifier = sqlparser.NewTableIdent(show.DbName)
	}
	table, _, _, _, destination, err := vschema.FindTableOrVindex(show.Table)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "table does not exists: %s", show.Table.Name.String())
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	// Remove Database Name from the query.
	show.DbName = ""
	show.Table.Qualifier = sqlparser.NewTableIdent("")
	show.Table.Name = table.Name

	return &engine.Send{
		Keyspace:          table.Keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}

func buildShowTableStatusPlan(show *sqlparser.ShowTableStatus, vschema ContextVSchema) (engine.Primitive, error) {
	destination, keyspace, _, err := vschema.TargetDestination(show.DatabaseName)
	if err != nil {
		return nil, err
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	// Remove Database Name from the query.
	show.DatabaseName = ""

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}
