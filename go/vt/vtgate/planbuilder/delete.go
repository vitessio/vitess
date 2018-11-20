/*
Copyright 2017 Google Inc.

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
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// buildDeletePlan builds the instructions for a DELETE statement.
func buildDeletePlan(del *sqlparser.Delete, vschema ContextVSchema) (*engine.Delete, error) {
	edel := &engine.Delete{
		Query: generateQuery(del),
	}
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(del)))
	if err := pb.processTableExprs(del.TableExprs); err != nil {
		return nil, err
	}
	rb, ok := pb.bldr.(*route)
	if !ok {
		return nil, errors.New("unsupported: multi-table/vindex delete statement in sharded keyspace")
	}
	edel.Keyspace = rb.ERoute.Keyspace
	if !edel.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		if !pb.validateSubquerySamePlan(del.Targets, del.Where, del.OrderBy, del.Limit) {
			return nil, errors.New("unsupported: sharded subqueries in DML")
		}
		edel.Opcode = engine.DeleteUnsharded
		return edel, nil
	}
	if del.Targets != nil || len(pb.st.tables) != 1 {
		return nil, errors.New("unsupported: multi-table delete statement in sharded keyspace")
	}
	if hasSubquery(del) {
		return nil, errors.New("unsupported: subqueries in sharded DML")
	}
	var tableName sqlparser.TableName
	for t := range pb.st.tables {
		tableName = t
	}
	table, _, destTabletType, destTarget, err := vschema.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	edel.Table = table

	directives := sqlparser.ExtractCommentDirectives(del.Comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		edel.MultiShardAutocommit = true
	}

	if destTarget != nil {
		if destTabletType != topodatapb.TabletType_MASTER {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported: DELETE statement with a replica target")
		}
		edel.Opcode = engine.DeleteByDestination
		edel.TargetDestination = destTarget
		return edel, nil
	}
	edel.Vindex, edel.Values, err = getDMLRouting(del.Where, edel.Table)
	// We couldn't generate a route for a single shard
	// Execute a delete sharded
	if err != nil {
		edel.Opcode = engine.DeleteScatter
	} else {
		edel.Opcode = engine.DeleteEqual
	}

	if edel.Opcode == engine.DeleteScatter {
		if len(edel.Table.Owned) != 0 {
			return edel, errors.New("unsupported: multi shard delete on a table with owned lookup vindexes")
		}
		if del.Limit != nil {
			return edel, errors.New("unsupported: multi shard delete with limit")
		}
	}

	edel.OwnedVindexQuery = generateDeleteSubquery(del, edel.Table)
	return edel, nil
}

// generateDeleteSubquery generates the query to fetch the rows
// that will be deleted. This allows VTGate to clean up any
// owned vindexes as needed.
func generateDeleteSubquery(del *sqlparser.Delete, table *vindexes.Table) string {
	if len(table.Owned) == 0 {
		return ""
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.WriteString("select ")
	for vIdx, cv := range table.Owned {
		for cIdx, column := range cv.Columns {
			if cIdx == 0 && vIdx == 0 {
				buf.Myprintf("%v", column)
			} else {
				buf.Myprintf(", %v", column)
			}
		}
	}
	buf.Myprintf(" from %v%v for update", table.Name, del.Where)
	return buf.String()
}
