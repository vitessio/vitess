/*
Copyright 2019 The Vitess Authors.

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

// buildDeletePlan builds the instructions for a DELETE statement.
func buildDeletePlan(stmt sqlparser.Statement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
	del := stmt.(*sqlparser.Delete)
	dml, ksidVindex, ksidCol, err := buildDMLPlan(vschema, "delete", del, reservedVars, del.TableExprs, del.Where, del.OrderBy, del.Limit, del.Comments, del.Targets)
	if err != nil {
		return nil, err
	}
	edel := &engine.Delete{
		DML: *dml,
	}

	if dml.Opcode == engine.Unsharded {
		return edel, nil
	}

	if len(del.Targets) > 1 {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "multi-table delete statement in not supported in sharded database")
	}

	if len(del.Targets) == 1 && del.Targets[0].Name != edel.Table.Name {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.UnknownTable, "Unknown table '%s' in MULTI DELETE", del.Targets[0].Name.String())
	}

	if len(edel.Table.Owned) > 0 {
		edel.OwnedVindexQuery = generateDMLSubquery(del.Where, del.OrderBy, del.Limit, edel.Table, ksidCol)
		edel.KsidVindex = ksidVindex
	}

	return edel, nil
}
