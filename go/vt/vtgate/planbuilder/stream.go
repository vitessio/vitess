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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildStreamPlan(stmt *sqlparser.Stream, vschema plancontext.VSchema) (*planResult, error) {
	table, _, destTabletType, dest, err := vschema.FindTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	if destTabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09009(destTabletType)
	}
	if dest == nil {
		dest = key.DestinationExactKeyRange{}
	}
	return newPlanResult(&engine.MStream{
		Keyspace:          table.Keyspace,
		TargetDestination: dest,
		TableName:         table.Name.CompliantName(),
	}), nil
}
