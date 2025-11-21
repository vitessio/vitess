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

package operators

import (
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func hasWindowFunctions(ctx *plancontext.PlanningContext) bool {
	stmt, ok := ctx.Statement.(*sqlparser.Select)
	if !ok {
		return false
	}

	var hasWindow bool
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.OverClause); ok {
			hasWindow = true
			return false, nil
		}
		return true, nil
	}, stmt)
	if err != nil {
		return false
	}
	return hasWindow
}

func validateWindowFunctionsForMultiShard(ctx *plancontext.PlanningContext, op Operator, routes []*Route) error {
	if len(routes) > 0 {
		for _, route := range routes {
			if err := validateRouteForWindows(ctx, route); err != nil {
				return err
			}
		}
		return nil
	}

	// Handle cases where window functions are used in UNION ALL or other complex operators
	if canExecuteWindowsInSubqueries(ctx, op) {
		return nil
	}

	return ctx.SemTable.NotSingleShardErr
}

func validateRouteForWindows(ctx *plancontext.PlanningContext, route *Route) error {
	if !canExecuteWindowsOnRoute(route) {
		return ctx.SemTable.NotSingleShardErr
	}
	if err := validateWindowPartitions(ctx, route); err != nil {
		return err
	}
	return nil
}

func canExecuteWindowsOnRoute(route *Route) bool {
	if route == nil {
		return false
	}

	// Allow window functions for single-shard opcodes
	if isSingleShardOpCode(route.Routing.OpCode()) {
		return true
	}

	// For sharded routing, require a unique vindex
	return hasUniqueSelectedVindex(route.Routing)
}

func isSingleShardOpCode(opCode engine.Opcode) bool {
	return opCode == engine.IN || opCode == engine.EqualUnique || opCode == engine.Unsharded ||
		opCode == engine.None || opCode == engine.DBA
}

func hasUniqueSelectedVindex(routing Routing) bool {
	shardedRouting, ok := routing.(*ShardedRouting)
	if !ok || shardedRouting.Selected == nil || shardedRouting.Selected.FoundVindex == nil {
		return false
	}
	return shardedRouting.Selected.FoundVindex.IsUnique()
}

func validateWindowPartitions(ctx *plancontext.PlanningContext, route *Route) error {
	stmt, ok := ctx.Statement.(*sqlparser.Select)
	if !ok {
		return nil
	}

	shardingColumns := findShardingColumns(route)
	if len(shardingColumns) == 0 {
		return nil
	}

	for _, expr := range stmt.SelectExprs.Exprs {
		if err := validateWindowPartitionBy(expr, shardingColumns); err != nil {
			return err
		}
	}

	if err := validateNamedWindows(stmt.Windows, shardingColumns); err != nil {
		return err
	}

	return nil
}

func canExecuteWindowsInSubqueries(ctx *plancontext.PlanningContext, op Operator) bool {
	if op == nil || ctx == nil {
		return true
	}

	hasInvalidSubquery := false

	visitF := func(op Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		if op == nil {
			return op, NoRewrite
		}

		if subOp, ok := op.(*SubQuery); ok {
			if subOp.Subquery == nil {
				return op, NoRewrite
			}
			if subRoute, ok := subOp.Subquery.(*Route); ok {
				if validateRouteForWindows(ctx, subRoute) != nil {
					hasInvalidSubquery = true
				}
			}
		}
		return op, NoRewrite
	}

	TopDown(op, TableID, visitF, stopAtRoute)
	return !hasInvalidSubquery
}

func findShardingColumns(route *Route) []string {
	shardedRouting, ok := route.Routing.(*ShardedRouting)
	if !ok {
		return nil
	}

	if shardedRouting.Selected != nil {
		for _, vpp := range shardedRouting.VindexPreds {
			if vpp.ColVindex.Vindex == shardedRouting.Selected.FoundVindex {
				return slice.Map(vpp.ColVindex.Columns, func(col sqlparser.IdentifierCI) string {
					return col.String()
				})
			}
		}
	}

	return nil
}

func validateNamedWindows(windows []*sqlparser.NamedWindow, shardingColumns []string) error {
	for _, namedWindow := range windows {
		for _, windowDef := range namedWindow.Windows {
			if err := validateWindowSpec(windowDef.WindowSpec, shardingColumns); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateWindowSpec(spec *sqlparser.WindowSpecification, shardingColumns []string) error {
	if len(shardingColumns) == 0 {
		return nil
	}

	if spec == nil || len(spec.PartitionClause) == 0 {
		return newPartitionByError(shardingColumns)
	}

	requiredColumns := make(map[string]bool, len(shardingColumns))
	for _, shardingCol := range shardingColumns {
		requiredColumns[strings.ToLower(shardingCol)] = true
	}

	for _, expr := range spec.PartitionClause {
		col, ok := expr.(*sqlparser.ColName)
		if !ok {
			continue
		}
		colName := strings.ToLower(col.Name.String())
		if requiredColumns[colName] {
			delete(requiredColumns, colName)
		}
	}

	if len(requiredColumns) > 0 {
		return newPartitionByError(shardingColumns)
	}

	return nil
}

func newPartitionByError(shardingColumns []string) error {
	return vterrors.VT12001("window function PARTITION BY must include all sharding key columns " + strings.Join(shardingColumns, ", ") + " for multi-shard queries")
}

func validateWindowPartitionBy(expr interface{}, shardingColumns []string) error {
	if expr == nil {
		return nil
	}

	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		overClause, ok := node.(*sqlparser.OverClause)
		if !ok {
			return true, nil
		}
		if err := validateWindowSpec(overClause.WindowSpec, shardingColumns); err != nil {
			return false, err
		}
		return true, nil
	}, expr.(sqlparser.SQLNode))
}
