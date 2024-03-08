/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"sort"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type DMLCommon struct {
	Ignore           sqlparser.Ignore
	Target           TargetTable
	OwnedVindexQuery *sqlparser.Select
	Source           Operator
}

type TargetTable struct {
	ID     semantics.TableSet
	VTable *vindexes.Table
	Name   sqlparser.TableName
}

// dmlOp stores intermediary value for Update/Delete Operator with the vindexes.Table for ordering.
type dmlOp struct {
	op   Operator
	vTbl *vindexes.Table
	cols []*sqlparser.ColName
}

// sortDmlOps sort the operator based on sharding vindex type.
// Unsharded < Lookup Vindex < Any
// This is needed to ensure all the rows are deleted from unowned sharding tables first.
// Otherwise, those table rows will be missed from getting deleted as
// the owned table row won't have matching values.
func sortDmlOps(dmlOps []dmlOp) []dmlOp {
	sort.Slice(dmlOps, func(i, j int) bool {
		a, b := dmlOps[i], dmlOps[j]
		// Get the first Vindex of a and b, if available
		aVdx, bVdx := getFirstVindex(a.vTbl), getFirstVindex(b.vTbl)

		// Sort nil Vindexes to the start
		if aVdx == nil || bVdx == nil {
			return aVdx != nil // true if bVdx is nil and aVdx is not nil
		}

		// Among non-nil Vindexes, those that need VCursor come first
		return aVdx.NeedsVCursor() && !bVdx.NeedsVCursor()
	})
	return dmlOps
}

func shortDesc(target TargetTable, ovq *sqlparser.Select) string {
	ovqString := ""
	if ovq != nil {
		var cols, orderby, limit string
		cols = fmt.Sprintf("COLUMNS: [%s]", sqlparser.String(ovq.SelectExprs))
		if len(ovq.OrderBy) > 0 {
			orderby = fmt.Sprintf(" ORDERBY: [%s]", sqlparser.String(ovq.OrderBy))
		}
		if ovq.Limit != nil {
			limit = fmt.Sprintf(" LIMIT: [%s]", sqlparser.String(ovq.Limit))
		}
		ovqString = fmt.Sprintf(" vindexQuery(%s%s%s)", cols, orderby, limit)
	}
	return fmt.Sprintf("%s.%s%s", target.VTable.Keyspace.Name, target.VTable.Name.String(), ovqString)
}

// getVindexInformation returns the vindex and VindexPlusPredicates for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getVindexInformation(id semantics.TableSet, table *vindexes.Table) *vindexes.ColumnVindex {
	// Check that we have a primary vindex which is valid
	if len(table.ColumnVindexes) == 0 || !table.ColumnVindexes[0].IsUnique() {
		panic(vterrors.VT09001(table.Name))
	}
	return table.ColumnVindexes[0]
}
