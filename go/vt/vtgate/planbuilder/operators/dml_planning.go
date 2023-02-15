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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// getVindexInformation returns the vindex and VindexPlusPredicates for the DML,
// If it cannot find a unique vindex match, it returns an error.
func getVindexInformation(
	id semantics.TableSet,
	predicates []sqlparser.Expr,
	table *vindexes.Table,
) (*vindexes.ColumnVindex, []*VindexPlusPredicates, error) {
	// Check that we have a primary vindex which is valid
	if len(table.ColumnVindexes) == 0 || !table.ColumnVindexes[0].IsUnique() {
		return nil, nil, vterrors.VT09001(table.Name)
	}
	primaryVindex := table.ColumnVindexes[0]
	if len(predicates) == 0 {
		return primaryVindex, nil, nil
	}

	var vindexesAndPredicates []*VindexPlusPredicates
	for _, colVindex := range table.Ordered {
		if lu, isLu := colVindex.Vindex.(vindexes.LookupBackfill); isLu && lu.IsBackfilling() {
			// Checking if the Vindex is currently backfilling or not, if it isn't we can read from the vindex table
			// and we will be able to do a delete equal. Otherwise, we continue to look for next best vindex.
			continue
		}

		vindexesAndPredicates = append(vindexesAndPredicates, &VindexPlusPredicates{
			ColVindex: colVindex,
			TableID:   id,
		})
	}
	return primaryVindex, vindexesAndPredicates, nil
}

func buildChangedVindexesValues(update *sqlparser.Update, table *vindexes.Table, ksidCols []sqlparser.IdentifierCI) (map[string]*engine.VindexValues, string, error) {
	changedVindexes := make(map[string]*engine.VindexValues)
	buf, offset := initialQuery(ksidCols, table)
	for i, vindex := range table.ColumnVindexes {
		vindexValueMap := make(map[string]evalengine.Expr)
		first := true
		for _, vcol := range vindex.Columns {
			// Searching in order of columns in colvindex.
			found := false
			for _, assignment := range update.Exprs {
				if !vcol.Equal(assignment.Name.Name) {
					continue
				}
				if found {
					return nil, "", vterrors.VT03015(assignment.Name.Name)
				}
				found = true
				pv, err := extractValueFromUpdate(assignment)
				if err != nil {
					return nil, "", err
				}
				vindexValueMap[vcol.String()] = pv
				if first {
					buf.Myprintf(", %v", assignment)
					first = false
				} else {
					buf.Myprintf(" and %v", assignment)
				}
			}
		}
		if len(vindexValueMap) == 0 {
			// Vindex not changing, continue
			continue
		}

		if update.Limit != nil && len(update.OrderBy) == 0 {
			return nil, "", vterrors.VT12001(fmt.Sprintf("you need to provide the ORDER BY clause when using LIMIT; invalid update on vindex: %v", vindex.Name))
		}
		if i == 0 {
			return nil, "", vterrors.VT12001(fmt.Sprintf("you cannot UPDATE primary vindex columns; invalid update on vindex: %v", vindex.Name))
		}
		if _, ok := vindex.Vindex.(vindexes.Lookup); !ok {
			return nil, "", vterrors.VT12001(fmt.Sprintf("you can only UPDATE lookup vindexes; invalid update on vindex: %v", vindex.Name))
		}
		changedVindexes[vindex.Name] = &engine.VindexValues{
			PvMap:  vindexValueMap,
			Offset: offset,
		}
		offset++
	}
	if len(changedVindexes) == 0 {
		return nil, "", nil
	}
	// generate rest of the owned vindex query.
	aTblExpr, ok := update.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, "", vterrors.VT12001("UPDATE on complex table expression")
	}
	tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: table.Name}, As: aTblExpr.As}
	buf.Myprintf(" from %v%v%v%v for update", tblExpr, update.Where, update.OrderBy, update.Limit)
	return changedVindexes, buf.String(), nil
}

func initialQuery(ksidCols []sqlparser.IdentifierCI, table *vindexes.Table) (*sqlparser.TrackedBuffer, int) {
	buf := sqlparser.NewTrackedBuffer(nil)
	offset := 0
	for _, col := range ksidCols {
		if offset == 0 {
			buf.Myprintf("select %v", col)
		} else {
			buf.Myprintf(", %v", col)
		}
		offset++
	}
	for _, cv := range table.Owned {
		for _, column := range cv.Columns {
			buf.Myprintf(", %v", column)
			offset++
		}
	}
	return buf, offset
}

// extractValueFromUpdate given an UpdateExpr, builds an evalengine.Expr
func extractValueFromUpdate(upd *sqlparser.UpdateExpr) (evalengine.Expr, error) {
	expr := upd.Expr
	if sq, ok := expr.(*sqlparser.ExtractedSubquery); ok {
		// if we are planning an update that needs one or more values from the outside, we can trust that they have
		// been correctly extracted from this query before we reach this far
		// if Merged is true, it means that this subquery was happily merged with the outer.
		// But in that case we should not be here, so we fail
		if sq.Merged {
			return nil, invalidUpdateExpr(upd, expr)
		}
		expr = sqlparser.NewArgument(sq.GetArgName())
	}

	pv, err := evalengine.Translate(expr, semantics.EmptySemTable())
	if err != nil || sqlparser.IsSimpleTuple(expr) {
		return nil, invalidUpdateExpr(upd, expr)
	}
	return pv, nil
}

func invalidUpdateExpr(upd *sqlparser.UpdateExpr, expr sqlparser.Expr) error {
	return vterrors.VT12001(fmt.Sprintf("only values are supported; invalid update on column: `%s` with expr: [%s]", upd.Name.Name.String(), sqlparser.String(expr)))
}
