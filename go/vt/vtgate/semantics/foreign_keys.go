/*
Copyright 2024 The Vitess Authors.

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

package semantics

import (
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type fkManager struct {
	binder   *binder
	tables   *tableCollector
	si       SchemaInformation
	getError func() error
}

// getInvolvedForeignKeys gets the foreign keys that might require taking care off when executing the given statement.
func (fk *fkManager) getInvolvedForeignKeys(statement sqlparser.Statement, fkChecksState *bool) (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, map[string]sqlparser.UpdateExprs, error) {
	if fkChecksState != nil && !*fkChecksState {
		return nil, nil, nil, nil
	}
	// There are only the DML statements that require any foreign keys handling.
	switch stmt := statement.(type) {
	case *sqlparser.Delete:
		// For DELETE statements, none of the parent foreign keys require handling.
		// So we collect all the child foreign keys.
		allChildFks, _, err := fk.getAllManagedForeignKeys()
		return allChildFks, nil, nil, err
	case *sqlparser.Insert:
		// For INSERT statements, we have 3 different cases:
		// 1. REPLACE statement: REPLACE statements are essentially DELETEs and INSERTs rolled into one.
		// 	  So we need to the parent foreign keys to ensure we are inserting the correct values, and the child foreign keys
		//	  to ensure we don't change a row that breaks the constraint or cascade any operations on the child tables.
		// 2. Normal INSERT statement: We don't need to check anything on the child foreign keys, so we just get all the parent foreign keys.
		// 3. INSERT with ON DUPLICATE KEY UPDATE: This might trigger an update on the columns specified in the ON DUPLICATE KEY UPDATE clause.
		allChildFks, allParentFKs, err := fk.getAllManagedForeignKeys()
		if err != nil {
			return nil, nil, nil, err
		}
		if stmt.Action == sqlparser.ReplaceAct {
			return allChildFks, allParentFKs, nil, nil
		}
		if len(stmt.OnDup) == 0 {
			return nil, allParentFKs, nil, nil
		}
		// If only a certain set of columns are being updated, then there might be some child foreign keys that don't need any consideration since their columns aren't being updated.
		// So, we filter these child foreign keys out. We can't filter any parent foreign keys because the statement will INSERT a row too, which requires validating all the parent foreign keys.
		updatedChildFks, _, childFkToUpdExprs, err := fk.filterForeignKeysUsingUpdateExpressions(allChildFks, nil, sqlparser.UpdateExprs(stmt.OnDup))
		return updatedChildFks, allParentFKs, childFkToUpdExprs, err
	case *sqlparser.Update:
		// For UPDATE queries we get all the parent and child foreign keys, but we can filter some of them out if the columns that they consist off aren't being updated or are set to NULLs.
		allChildFks, allParentFks, err := fk.getAllManagedForeignKeys()
		if err != nil {
			return nil, nil, nil, err
		}
		return fk.filterForeignKeysUsingUpdateExpressions(allChildFks, allParentFks, stmt.Exprs)
	default:
		return nil, nil, nil, nil
	}
}

// filterForeignKeysUsingUpdateExpressions filters the child and parent foreign key constraints that don't require any validations/cascades given the updated expressions.
func (fk *fkManager) filterForeignKeysUsingUpdateExpressions(allChildFks map[TableSet][]vindexes.ChildFKInfo, allParentFks map[TableSet][]vindexes.ParentFKInfo, updExprs sqlparser.UpdateExprs) (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, map[string]sqlparser.UpdateExprs, error) {
	if len(allChildFks) == 0 && len(allParentFks) == 0 {
		return nil, nil, nil, nil
	}

	pFksRequired := make(map[TableSet][]bool, len(allParentFks))
	cFksRequired := make(map[TableSet][]bool, len(allChildFks))
	for ts, fks := range allParentFks {
		pFksRequired[ts] = make([]bool, len(fks))
	}
	for ts, fks := range allChildFks {
		cFksRequired[ts] = make([]bool, len(fks))
	}

	// updExprToTableSet stores the tables that the updated expressions are from.
	updExprToTableSet := make(map[*sqlparser.ColName]TableSet)

	// childFKToUpdExprs stores child foreign key to update expressions mapping.
	childFKToUpdExprs := map[string]sqlparser.UpdateExprs{}

	// Go over all the update expressions
	for _, updateExpr := range updExprs {
		deps := fk.binder.direct.dependencies(updateExpr.Name)
		if deps.NumberOfTables() != 1 {
			// If we don't get exactly one table for the given update expression, we would have definitely run into an error
			// during the binder phase that we would have stored. We should return that error, since we can't safely proceed with
			// foreign key related changes without having all the information.
			return nil, nil, nil, fk.getError()
		}
		updExprToTableSet[updateExpr.Name] = deps
		// Get all the child and parent foreign keys for the given table that the update expression belongs to.
		childFks := allChildFks[deps]
		parentFKs := allParentFks[deps]

		// Any foreign key to a child table for a column that has been updated
		// will require the cascade operations or restrict verification to happen, so we include all such foreign keys.
		for idx, childFk := range childFks {
			if childFk.ParentColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				cFksRequired[deps][idx] = true
				tbl, _ := fk.tables.tableInfoFor(deps)
				ue := childFKToUpdExprs[childFk.String(tbl.GetVindexTable())]
				ue = append(ue, updateExpr)
				childFKToUpdExprs[childFk.String(tbl.GetVindexTable())] = ue
			}
		}
		// If we are setting a column to NULL, then we don't need to verify the existence of an
		// equivalent row in the parent table, even if this column was part of a foreign key to a parent table.
		if sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		// We add all the possible parent foreign key constraints that need verification that an equivalent row
		// exists, given that this column has changed.
		for idx, parentFk := range parentFKs {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[deps][idx] = true
			}
		}
	}
	// For the parent foreign keys, if any of the columns part of the fk is set to NULL,
	// then, we don't care for the existence of an equivalent row in the parent table.
	for _, updateExpr := range updExprs {
		if !sqlparser.IsNull(updateExpr.Expr) {
			continue
		}
		ts := updExprToTableSet[updateExpr.Name]
		parentFKs := allParentFks[ts]
		for idx, parentFk := range parentFKs {
			if parentFk.ChildColumns.FindColumn(updateExpr.Name.Name) >= 0 {
				pFksRequired[ts][idx] = false
			}
		}
	}

	// Create new maps with only the required foreign keys.
	pFksNeedsHandling := map[TableSet][]vindexes.ParentFKInfo{}
	cFksNeedsHandling := map[TableSet][]vindexes.ChildFKInfo{}
	for ts, parentFks := range allParentFks {
		var pFKNeeded []vindexes.ParentFKInfo
		for idx, fk := range parentFks {
			if pFksRequired[ts][idx] {
				pFKNeeded = append(pFKNeeded, fk)
			}
		}
		pFksNeedsHandling[ts] = pFKNeeded
	}
	for ts, childFks := range allChildFks {
		var cFKNeeded []vindexes.ChildFKInfo
		for idx, fk := range childFks {
			if cFksRequired[ts][idx] {
				cFKNeeded = append(cFKNeeded, fk)
			}
		}
		cFksNeedsHandling[ts] = cFKNeeded
	}
	return cFksNeedsHandling, pFksNeedsHandling, childFKToUpdExprs, nil
}

// getAllManagedForeignKeys gets all the foreign keys for the query we are analyzing that Vitess is responsible for managing.
func (fk *fkManager) getAllManagedForeignKeys() (map[TableSet][]vindexes.ChildFKInfo, map[TableSet][]vindexes.ParentFKInfo, error) {
	allChildFKs := make(map[TableSet][]vindexes.ChildFKInfo)
	allParentFKs := make(map[TableSet][]vindexes.ParentFKInfo)

	// Go over all the tables and collect the foreign keys.
	for idx, table := range fk.tables.Tables {
		vi := table.GetVindexTable()
		if vi == nil || vi.Keyspace == nil {
			// If is not a real table, so should be skipped.
			continue
		}
		// Check whether Vitess needs to manage the foreign keys in this keyspace or not.
		fkMode, err := fk.si.ForeignKeyMode(vi.Keyspace.Name)
		if err != nil {
			return nil, nil, err
		}
		if fkMode != vschemapb.Keyspace_managed {
			continue
		}
		// Cyclic foreign key constraints error is stored in the keyspace.
		ksErr := fk.si.KeyspaceError(vi.Keyspace.Name)
		if ksErr != nil {
			return nil, nil, ksErr
		}

		// Add all the child and parent foreign keys to our map.
		ts := SingleTableSet(idx)
		allChildFKs[ts] = vi.ChildForeignKeys
		allParentFKs[ts] = vi.ParentForeignKeys
	}
	return allChildFKs, allParentFKs, nil
}
