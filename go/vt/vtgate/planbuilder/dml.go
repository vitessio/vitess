// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func buildUpdatePlan(upd *sqlparser.Update, schema *Schema) *Plan {
	plan := &Plan{
		ID:        NoPlan,
		Rewritten: generateQuery(upd),
	}
	tablename := sqlparser.GetTableName(upd.Table)
	plan.Table, plan.Reason = schema.LookupTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if plan.Table.Keyspace.ShardingScheme == Unsharded {
		plan.ID = UpdateUnsharded
		return plan
	}

	getWhereRouting(upd.Where, plan)
	switch plan.ID {
	case SelectSingleShardKey:
		plan.ID = UpdateSingleShardKey
	case SelectSingleLookup:
		plan.ID = UpdateSingleLookup
	case SelectMultiShardKey, SelectMultiLookup, SelectScatter:
		plan.ID = NoPlan
		plan.Reason = "too complex"
		return plan
	default:
		panic("unexpected")
	}
	if isIndexChanging(upd.Exprs, plan.Table.Indexes) {
		plan.ID = NoPlan
		plan.Reason = "index is changing"
	}
	return plan
}

func isIndexChanging(setClauses sqlparser.UpdateExprs, indexes []*Index) bool {
	indexCols := make([]string, len(indexes))
	for i, index := range indexes {
		indexCols[i] = index.Column
	}
	for _, assignment := range setClauses {
		if sqlparser.StringIn(string(assignment.Name.Name), indexCols...) {
			return true
		}
	}
	return false
}

func buildDeletePlan(del *sqlparser.Delete, schema *Schema) *Plan {
	plan := &Plan{
		ID:        NoPlan,
		Rewritten: generateQuery(del),
	}
	tablename := sqlparser.GetTableName(del.Table)
	plan.Table, plan.Reason = schema.LookupTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if plan.Table.Keyspace.ShardingScheme == Unsharded {
		plan.ID = DeleteUnsharded
		return plan
	}

	getWhereRouting(del.Where, plan)
	switch plan.ID {
	case SelectSingleShardKey:
		plan.ID = DeleteSingleShardKey
	case SelectSingleLookup:
		plan.ID = DeleteSingleLookup
	case SelectMultiShardKey, SelectMultiLookup, SelectScatter:
		plan.ID = NoPlan
		plan.Reason = "too complex"
	default:
		panic("unexpected")
	}
	return plan
}
