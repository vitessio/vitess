/*
Copyright 2021 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildCallProcPlan(stmt *sqlparser.CallProc, vschema ContextVSchema) (engine.Primitive, error) {
	var ks string
	if stmt.Name.Qualifier.IsEmpty() {
		keyspace, err := vschema.DefaultKeyspace()
		if err != nil {
			return nil, err
		}
		ks = keyspace.Name
	} else {
		ks = stmt.Name.Qualifier.String()
	}

	destination, keyspace, _, err := vschema.TargetDestination(ks)
	if err != nil {
		return nil, err
	}

	if keyspace.Sharded {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, errNotAllowWhenSharded)
	}

	stmt.Name.Qualifier = sqlparser.NewTableIdent("")

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(stmt),
		IsDML:             false,
		SingleShardOnly:   false,
	}, nil
}

const errNotAllowWhenSharded = "CALL is only allowed for targeted queries or on unsharded keyspaces"
