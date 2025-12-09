/*
Copyright 2025 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func transformWindow(ctx *plancontext.PlanningContext, op *operators.Window) (engine.Primitive, error) {
	prim, err := transformToPrimitive(ctx, op.Source)
	if err != nil {
		return nil, err
	}

	// Multi-source primitives (Join, HashJoin, ValuesJoin, SemiJoin, Concatenate, Sequential)
	// cannot guarantee partitions stay on single shard
	switch prim.(type) {
	case *engine.Join, *engine.HashJoin, *engine.ValuesJoin, *engine.SemiJoin, *engine.Concatenate, *engine.Sequential:
		return nil, vterrors.VT12001("window functions are only supported for single-shard queries")
	}

	// Multi-shard routes OK only if partitioned by unique vindex (guarantees same-shard partition rows)
	// E.g., PARTITION BY id (primary vindex) OK; PARTITION BY region NOT OK
	if route, ok := prim.(*engine.Route); ok && !isSingleShardPrimitive(route) {
		if routeOp, ok := op.Source.(*operators.Route); ok {
			if canPushDownWindow(op, routeOp) {
				// Partition is based on unique vindex - safe to execute on multi-shard route
				return prim, nil
			}
		}
		return nil, vterrors.VT12001("window functions are only supported for single-shard queries")
	}

	return prim, nil
}

func isSingleShardPrimitive(route *engine.Route) bool {
	if route == nil || route.RoutingParameters == nil {
		return false
	}
	return route.RoutingParameters.Opcode.IsSingleShard()
}

type windowTableInfo struct {
	vTable *vindexes.BaseTable
	alias  sqlparser.IdentifierCS
}

// canPushDownWindow checks if a window function partitions by a unique vindex.
// Returns false if PARTITION BY is missing or covers non-vindex columns.
// Examples:
//
//	OK: SELECT ... FROM user WHERE id=1 PARTITION BY id (single shard)
//	OK: SELECT ... FROM user PARTITION BY id (id is primary vindex, same-shard partitions)
//	NO: SELECT ... FROM user PARTITION BY region (region scattered across shards)
func canPushDownWindow(op *operators.Window, route *operators.Route) bool {
	// Collect tables with their aliases
	var tables []windowTableInfo
	_ = operators.Visit(route, func(o operators.Operator) error {
		if t, ok := o.(*operators.Table); ok && t.VTable != nil {
			alias := t.QTable.Alias.As
			if alias.IsEmpty() {
				alias = sqlparser.NewIdentifierCS(t.QTable.Table.Name.String())
			}
			tables = append(tables, windowTableInfo{vTable: t.VTable, alias: alias})
		}
		return nil
	})

	// Collect window functions from SELECT expressions
	var windowFuncs []sqlparser.WindowFunc
	for _, expr := range op.QP.SelectExprs {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if wf, ok := node.(sqlparser.WindowFunc); ok {
				windowFuncs = append(windowFuncs, wf)
			}
			return true, nil
		}, expr.Col)
	}

	if len(windowFuncs) == 0 {
		return true
	}

	// Validate each window function partitions by unique vindex
	for _, wf := range windowFuncs {
		if !isPartitionedByUniqueVindex(wf, tables) {
			return false
		}
	}

	return true
}

// isPartitionedByUniqueVindex checks if a window function's PARTITION BY covers:
//  1. Primary vindex columns (ensures same-shard partitions), or
//  2. Unique vindex columns (each partition has ≤1 row, trivially single-shard)
func isPartitionedByUniqueVindex(wf sqlparser.WindowFunc, tables []windowTableInfo) bool {
	overClause := wf.GetOverClause()
	if overClause == nil || overClause.WindowSpec == nil || len(overClause.WindowSpec.PartitionClause) == 0 {
		return false
	}

	partitionBy := overClause.WindowSpec.PartitionClause

	for _, table := range tables {
		if len(table.vTable.ColumnVindexes) == 0 {
			continue
		}

		// Pre-build column lookup map for column validation
		var columnSet map[string]bool
		if table.vTable.ColumnListAuthoritative {
			columnSet = make(map[string]bool, len(table.vTable.Columns))
			for _, col := range table.vTable.Columns {
				columnSet[col.Name.Lowered()] = true
			}
		}

		// Build set of partition columns matching this table - O(p) where p = partition columns
		coveredCols := make(map[string]bool)
		for _, pExpr := range partitionBy {
			colName, ok := pExpr.(*sqlparser.ColName)
			if !ok {
				continue
			}

			// Skip if qualified to different table
			if !colName.Qualifier.IsEmpty() && colName.Qualifier.Name.String() != table.alias.String() {
				continue
			}

			// Validate column exists in schema if authoritative - O(1) lookup instead of O(c)
			if columnSet != nil {
				if !columnSet[colName.Name.Lowered()] {
					if !colName.Qualifier.IsEmpty() || len(tables) == 1 {
						return false
					}
					continue
				}
			}

			coveredCols[colName.Name.Lowered()] = true
		}

		checkVindex := func(vindex *vindexes.ColumnVindex) bool {
			for _, vCol := range vindex.Columns {
				if !coveredCols[vCol.Lowered()] {
					return false
				}
			}
			return true
		}

		// Check primary vindex (determines shard routing)
		primaryVindex := table.vTable.ColumnVindexes[0]
		if checkVindex(primaryVindex) {
			return true
		}

		// Check unique vindexes (each partition has ≤1 row)
		for _, vindex := range table.vTable.ColumnVindexes[1:] {
			if vindex.IsUnique() && checkVindex(vindex) {
				return true
			}
		}
	}
	return false
}
