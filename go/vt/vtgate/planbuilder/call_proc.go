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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildCallProcPlan(stmt *sqlparser.CallProc, vschema plancontext.VSchema) (*planResult, error) {
	var ks string
	if stmt.Name.Qualifier.NotEmpty() {
		ks = stmt.Name.Qualifier.String()
	}

	dest, keyspace, _, err := vschema.TargetDestination(ks)
	if err != nil {
		return nil, err
	}

	if dest == nil {
		if err := vschema.ErrorIfShardedF(keyspace, "CALL", errNotAllowWhenSharded); err != nil {
			return nil, err
		}
		dest = key.DestinationAnyShard{}
	}

	stmt.Name.Qualifier = sqlparser.NewIdentifierCS("")

	return newPlanResult(&engine.Send{
		Keyspace:          keyspace,
		TargetDestination: dest,
		Query:             sqlparser.String(stmt),
	}), nil
}

const errNotAllowWhenSharded = "CALL is not supported for sharded keyspace"
