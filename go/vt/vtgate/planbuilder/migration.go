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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildAlterMigrationPlan(query string, vschema ContextVSchema) (engine.Primitive, error) {
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "ALTER VITESS_MIGRATION works only on primary tablet")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}, nil
}

func buildRevertMigrationPlan(query string, stmt *sqlparser.RevertMigration, vschema ContextVSchema) (engine.Primitive, error) {
	_, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "REVERT VITESS_MIGRATION works only on primary tablet")
	}

	return &engine.RevertMigration{
		Keyspace: ks,
		Stmt:     stmt,
		Query:    query,
	}, nil
}
