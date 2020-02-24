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
func getDMLRouting(where *sqlparser.Where, table *vindexes.Table) (engine.DMLOpcode, vindexes.SingleColumn, string, []sqltypes.PlanValue) {
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
		}

		if where == nil {
			return engine.Scatter, nil, ksidCol, nil
		}

		if pv, ok := getMatch(where.Expr, index.Columns[0]); ok {
			if pv.IsList() {
				return engine.Scatter, nil, ksidCol, nil
			}

			return engine.Equal, single, ksidCol, []sqltypes.PlanValue{pv}
		}
	}
	return engine.Scatter, nil, ksidCol, nil
}

// getMatch returns the matched value if there is an equality
// constraint on the specified column that can be used to
// decide on a route.
func getMatch(node sqlparser.Expr, col sqlparser.ColIdent) (pv sqltypes.PlanValue, ok bool) {
	filters := splitAndExpression(nil, node)
	for _, filter := range filters {
		filter = skipParenthesis(filter)
		if parenthesis, ok := node.(*sqlparser.ParenExpr); ok {
			if pv, ok := getMatch(parenthesis.Expr, col); ok {
				return pv, ok
			}
			continue
		}
		comparison, ok := filter.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		if !nameMatch(comparison.Left, col) {
			continue
		}
		switch comparison.Operator {
		case sqlparser.EqualStr:
			if !sqlparser.IsValue(comparison.Right) {
				continue
			}
		case sqlparser.InStr:
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

func buildDMLPlan(vschema ContextVSchema, dmlType string, stmt sqlparser.Statement, tableExprs sqlparser.TableExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy, limit *sqlparser.Limit, comments sqlparser.Comments, nodes ...sqlparser.SQLNode) (*engine.DML, string, error) {
	eupd := &engine.DML{}
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(stmt)))
	ro, err := pb.processDMLTable(tableExprs)
	if err != nil {
		return nil, "", err
	}
	eupd.Keyspace = ro.eroute.Keyspace
	if !eupd.Keyspace.Sharded {
		// We only validate non-table subexpressions because the previous analysis has already validated them.
		var subqueryArgs []sqlparser.SQLNode
		subqueryArgs = append(subqueryArgs, nodes...)
		subqueryArgs = append(subqueryArgs, where, orderBy, limit)
		if !pb.finalizeUnshardedDMLSubqueries(subqueryArgs...) {
			return nil, "", vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: sharded subqueries in DML")
		}
		eupd.Opcode = engine.Unsharded
		// Generate query after all the analysis. Otherwise table name substitutions for
		// routed tables won't happen.
		eupd.Query = generateQuery(stmt)
		return eupd, "", nil
	}

	if hasSubquery(stmt) {
		return nil, "", vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: subqueries in sharded DML")
	}

	if len(pb.st.tables) != 1 {
		return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multi-table %s statement in sharded keyspace", dmlType)
	}

	// Generate query after all the analysis. Otherwise table name substitutions for
	// routed tables won't happen.
	eupd.Query = generateQuery(stmt)

	directives := sqlparser.ExtractCommentDirectives(comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		eupd.MultiShardAutocommit = true
	}

	eupd.QueryTimeout = queryTimeout(directives)
	eupd.Table = ro.vschemaTable
	if eupd.Table == nil {
		return nil, "", vterrors.New(vtrpcpb.Code_INTERNAL, "internal error: table.vindexTable is mysteriously nil")
	}

	if ro.eroute.TargetDestination != nil {
		if ro.eroute.TargetTabletType != topodatapb.TabletType_MASTER {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported: %s statement with a replica target", dmlType)
		}
		eupd.Opcode = engine.ByDestination
		eupd.TargetDestination = ro.eroute.TargetDestination
		return eupd, "", nil
	}

	routingType, vindex, ksidCol, values := getDMLRouting(where, eupd.Table)
	eupd.Opcode = routingType
	if routingType == engine.Scatter {
		if limit != nil {
			return nil, "", vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: multi shard %s with limit", dmlType)
		}
	} else {
		eupd.Values = values
		eupd.Vindex = vindex
	}

	return eupd, ksidCol, nil
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
