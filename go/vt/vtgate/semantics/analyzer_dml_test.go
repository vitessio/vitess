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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestUpdBindingColName(t *testing.T) {
	queries := []string{
		"update tabl set col = 1",
		"update t2 set uid = 5",
		"update tabl set tabl.col = 1 ",
		"update tabl set d.tabl.col = 5",
		"update d.tabl set col = 1",
		"update d.tabl set tabl.col = 5",
		"update d.tabl set d.tabl.col = 1",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "d")
			upd, _ := stmt.(*sqlparser.Update)
			t1 := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.Equal(t, SingleTableSet(0), ts)

			updExpr := extractFromUpdateSet(upd, 0)
			recursiveDeps := semTable.RecursiveDeps(updExpr.Name)
			assert.Equal(t, TS0, recursiveDeps, query)
			assert.Equal(t, TS0, semTable.DirectDeps(updExpr.Name), query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")

			recursiveDeps = semTable.RecursiveDeps(updExpr.Expr)
			assert.Equal(t, None, recursiveDeps, query)
			assert.Equal(t, None, semTable.DirectDeps(updExpr.Expr), query)
		})
	}
}

func TestUpdBindingExpr(t *testing.T) {
	queries := []string{
		"update tabl set col = col",
		"update tabl set d.tabl.col = tabl.col",
		"update d.tabl set col = d.tabl.col",
		"update d.tabl set tabl.col = col",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "d")
			upd, _ := stmt.(*sqlparser.Update)
			t1 := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.Equal(t, SingleTableSet(0), ts)

			updExpr := extractFromUpdateSet(upd, 0)
			recursiveDeps := semTable.RecursiveDeps(updExpr.Name)
			assert.Equal(t, TS0, recursiveDeps, query)
			assert.Equal(t, TS0, semTable.DirectDeps(updExpr.Name), query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")

			recursiveDeps = semTable.RecursiveDeps(updExpr.Expr)
			assert.Equal(t, TS0, recursiveDeps, query)
			assert.Equal(t, TS0, semTable.DirectDeps(updExpr.Expr), query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func extractFromUpdateSet(in *sqlparser.Update, idx int) *sqlparser.UpdateExpr {
	return in.Exprs[idx]
}
