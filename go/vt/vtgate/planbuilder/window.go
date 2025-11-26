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

	if isSingleShard(prim) {
		return prim, nil
	}

	// If not single shard, check if we can push down based on partitioning
	if routeOp, ok := op.Source.(*operators.Route); ok {
		if canPushDownWindow(op, routeOp) {
			return prim, nil
		}
	}

	return nil, vterrors.VT12001("window functions are only supported for single-shard queries")
}

type windowTableInfo struct {
	vTable *vindexes.BaseTable
	alias  sqlparser.IdentifierCS
}

// canPushDownWindow checks if a window function can be safely pushed down (executed on) a single shard.
// It validates that all window functions in the query partition by columns that cover a sharding key,
// ensuring all rows for any partition will be on the same shard.
func canPushDownWindow(op *operators.Window, route *operators.Route) bool {
	// 1. Find all tables in the route with their aliases
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

	if len(tables) == 0 {
		return false
	}

	// 2. Get all window functions
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

	// 3. Check each window function
	for _, wf := range windowFuncs {
		if !isPartitionedByShardingKey(wf, tables) {
			return false
		}
	}

	return true
}

// isPartitionedByShardingKey checks if a window function's PARTITION BY clause covers a sharding key.
// It supports two cases:
//  1. Primary Vindex (index 0): If partition covers all columns of the primary vindex, all rows for
//     a partition are guaranteed to be on the same shard (since primary vindex determines shard routing).
//  2. Unique Vindexes (index 1+): If partition covers all columns of a unique vindex, each partition
//     contains at most one row.
func isPartitionedByShardingKey(wf sqlparser.WindowFunc, tables []windowTableInfo) bool {
	overClause := wf.GetOverClause()
	if overClause == nil || overClause.WindowSpec == nil || len(overClause.WindowSpec.PartitionClause) == 0 {
		return false
	}

	partitionBy := overClause.WindowSpec.PartitionClause

	for _, table := range tables {
		if len(table.vTable.ColumnVindexes) == 0 {
			continue
		}

		// Optimization: Build a set of column names from partitionBy that match the table
		coveredCols := make(map[string]bool)
		for _, pExpr := range partitionBy {
			colName, ok := pExpr.(*sqlparser.ColName)
			if !ok {
				continue
			}

			// If the column is qualified, check if it matches the table alias
			if !colName.Qualifier.IsEmpty() && colName.Qualifier.Name.String() != table.alias.String() {
				continue
			}

			// Check if the column exists in the table schema
			if table.vTable.ColumnListAuthoritative {
				found := false
				for _, col := range table.vTable.Columns {
					if col.Name.Equal(colName.Name) {
						found = true
						break
					}
				}
				if !found {
					// If the column is qualified and not found, it's an error (or at least not this table's column)
					// If it's unqualified and not found, it belongs to another table
					if !colName.Qualifier.IsEmpty() {
						return false
					}
					if len(tables) == 1 {
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

		// 1. Check Primary Vindex (Sharding Key), which is always at index 0.
		// We don't check IsUnique() here because even if it's not unique,
		// it determines the shard, so all rows for a partition will be on the same shard.
		primaryVindex := table.vTable.ColumnVindexes[0]
		if checkVindex(primaryVindex) {
			return true
		}

		// 2. Check other Unique Vindexes.
		// If a partition covers a unique vindex, the partition contains at most one row,
		// which is trivially single-shard.
		for _, vindex := range table.vTable.ColumnVindexes[1:] {
			if vindex.IsUnique() && checkVindex(vindex) {
				return true
			}
		}
	}
	return false
}
