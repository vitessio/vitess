// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

func buildUpdatePlan(upd *sqlparser.Update, schema *Schema) *Plan {
	tablename := sqlparser.GetTableName(upd.Table)
	plan := getTableRouting(tablename, schema)
	if plan != nil {
		if plan.ID == SelectUnsharded {
			plan.ID = UpdateUnsharded
		}
		plan.Query = generateQuery(upd)
		return plan
	}

	indexes := schema.Tables[tablename].Indexes
	plan = getWhereRouting(upd.Where, indexes)
	switch plan.ID {
	case SelectSingleShardKey:
		plan.ID = UpdateSingleShardKey
	case SelectSingleLookup:
		plan.ID = UpdateSingleLookup
	case SelectMultiShardKey, SelectMultiLookup, SelectScatter:
		return &Plan{
			ID:        NoPlan,
			Reason:    "too complex",
			TableName: tablename,
			Query:     generateQuery(upd),
		}
	default:
		panic("unexpected")
	}
	if isIndexChanging(upd.Exprs, indexes) {
		return &Plan{
			ID:        NoPlan,
			Reason:    "index is changing",
			TableName: tablename,
			Query:     generateQuery(upd),
		}
	}
	plan.TableName = tablename
	plan.Query = generateQuery(upd)
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
	tablename := sqlparser.GetTableName(del.Table)
	plan := getTableRouting(tablename, schema)
	if plan != nil {
		if plan.ID == SelectUnsharded {
			plan.ID = DeleteUnsharded
		}
		plan.Query = generateQuery(del)
		return plan
	}

	indexes := schema.Tables[tablename].Indexes
	plan = getWhereRouting(del.Where, indexes)
	switch plan.ID {
	case SelectSingleShardKey:
		plan.ID = DeleteSingleShardKey
	case SelectSingleLookup:
		plan.ID = DeleteSingleLookup
	case SelectMultiShardKey, SelectMultiLookup, SelectScatter:
		return &Plan{
			ID:        NoPlan,
			Reason:    "too complex",
			TableName: tablename,
			Query:     generateQuery(del),
		}
	default:
		panic("unexpected")
	}
	plan.TableName = tablename
	plan.Query = generateQuery(del)
	return plan
}
