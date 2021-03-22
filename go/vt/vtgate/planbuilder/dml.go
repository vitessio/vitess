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
	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// getDMLRouting returns the vindex and values for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getDMLRouting(where *sqlparser.Where, table *vindexes.Table) (engine.DMLOpcode, vindexes.SingleColumn, string, vindexes.SingleColumn, []sqltypes.PlanValue, error) {
	var ksidVindex vindexes.SingleColumn
	var ksidCol string
	for _, index := range table.Ordered {
		if !index.Vindex.IsUnique() {
			continue
		}
		single, ok := index.Vindex.(vindexes.SingleColumn)
		if !ok {
			continue
		}
		if ksidCol == "" {
			ksidCol = sqlparser.String(index.Columns[0])
			ksidVindex = single
		}
		if where == nil {
			return engine.Scatter, ksidVindex, ksidCol, nil, nil, nil
		}

		if pv, ok := getMatch(where.Expr, index.Columns[0]); ok {
			opcode := engine.Equal
			if pv.IsList() {
				opcode = engine.In
			}
			return opcode, ksidVindex, ksidCol, single, []sqltypes.PlanValue{pv}, nil
		}
	}
	if ksidVindex == nil {
		return engine.Scatter, nil, "", nil, nil, vterrors.New(vtrpcpb.Code_INTERNAL, "table without a primary vindex is not expected")
	}
	return engine.Scatter, ksidVindex, ksidCol, nil, nil, nil
}

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
	filters := splitAndExpression(nil, node)
	for _, filter := range filters {
		comparison, ok := filter.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		if !nameMatch(comparison.Left, col) {
			continue
		}
		switch comparison.Operator {
		case sqlparser.EqualOp:
			if !sqlparser.IsValue(comparison.Right) {
				continue
			}
		case sqlparser.InOp:
			if !sqlparser.IsSimpleTuple(comparison.Right) {
				continue
			}
		default:
			continue
		}
		pv, err := sqlparser.NewPlanValue(comparison.Right)
		if err != nil {
			continue
		}
		return pv, true
	}
	return sqltypes.PlanValue{}, false
}

func nameMatch(node sqlparser.Expr, col sqlparser.ColIdent) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && colname.Name.Equal(col)
}

func buildDMLPlan(vschema ContextVSchema, dmlType string, stmt sqlparser.Statement, reservedVars sqlparser.BindVars, tableExprs sqlparser.TableExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit, comments sqlparser.Comments, nodes ...sqlparser.SQLNode) (*engine.DML, vindexes.SingleColumn, string, error) {
	edml := &engine.DML{}
	pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
	rb, err := pb.processDMLTable(tableExprs, reservedVars, nil)
	if err != nil {
		return nil, nil, "", err
	}
	edml.Keyspace = rb.eroute.Keyspace
	if !edml.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		var subqueryArgs []sqlparser.SQLNode
		subqueryArgs = append(subqueryArgs, nodes...)
		subqueryArgs = append(subqueryArgs, where, orderBy, limit)
		if pb.finalizeUnshardedDMLSubqueries(reservedVars, subqueryArgs...) {
			vschema.WarnUnshardedOnly("subqueries can't be sharded in DML")
		} else {
			return nil, nil, "", vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: sharded subqueries in DML")
		}
		edml.Opcode = engine.Unsharded
		// Generate query after all the analysis. Otherwise table name substitutions for
		// routed tables won't happen.
		edml.Query = generateQuery(stmt)
		return edml, nil, "", nil
	}

	if hasSubquery(stmt) {
		return nil, nil, "", vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: subqueries in sharded DML")
	}

	// Generate query after all the analysis. Otherwise table name substitutions for
	// routed tables won't happen.
	edml.Query = generateQuery(stmt)

	directives := sqlparser.ExtractCommentDirectives(comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		edml.MultiShardAutocommit = true
	}

	edml.QueryTimeout = queryTimeout(directives)

	if len(pb.st.tables) != 1 {
		return nil, nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi-table %s statement is not supported in sharded database", dmlType)
	}
	for _, tval := range pb.st.tables {
		// There is only one table.
		edml.Table = tval.vschemaTable
	}

	routingType, ksidVindex, ksidCol, vindex, values, err := getDMLRouting(where, edml.Table)
	if err != nil {
		return nil, nil, "", err
	}

	if rb.eroute.TargetDestination != nil {
		if rb.eroute.TargetTabletType != topodatapb.TabletType_MASTER {
			return nil, nil, "", vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.InnodbReadOnly, "unsupported: %s statement with a replica target", dmlType)
		}
		edml.Opcode = engine.ByDestination
		edml.TargetDestination = rb.eroute.TargetDestination
		return edml, ksidVindex, ksidCol, nil
	}

	edml.Opcode = routingType
	if routingType == engine.Scatter {
		if limit != nil {
			return nil, nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi shard %s with limit is not supported", dmlType)
		}
	} else {
		edml.Vindex = vindex
		edml.Values = values
	}

	return edml, ksidVindex, ksidCol, nil
}

func generateDMLSubquery(where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit, table *vindexes.Table, ksidCol string) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select %s", ksidCol)
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			buf.Myprintf(", %v", column)
		}
	}
	buf.Myprintf(" from %v%v%v%v for update", table.Name, where, orderBy, limit)
	return buf.String()
}

func generateQuery(statement sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(dmlFormatter)
	statement.Format(buf)
	return buf.String()
}

// dmlFormatter strips out keyspace name from dmls.
func dmlFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.TableName:
		node.Name.Format(buf)
		return
	}
	node.Format(buf)
}
