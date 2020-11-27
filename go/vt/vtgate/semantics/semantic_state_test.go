/*
Copyright 2020 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSimpleColumn(t *testing.T) {
	query := "select col, (select 2) from (select col from t) as x where 3 in (select 4 from t where t.col = x.col)"
	stmt, semTable := parseAndAnalyze(t, query)
	sel, _ := stmt.(*sqlparser.Select)

	s1 := semTable.scope(sel.SelectExprs[0].(*sqlparser.AliasedExpr).Expr)
	s2 := semTable.scope(sel.From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.DerivedTable).Select.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr)
	require.NotEqual(t, fmt.Sprintf("%p", s1), fmt.Sprintf("%p", s2), "different scope expected")
}

/*func TestScoping(t *testing.T) {
	query :=
		`select (
	select 42
	from dual) as x
from (
	select col2 as col
	from t) as derived
where c = (
	select count(*)
	from t2
	where derived.col2 = t2.col2)`
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse)
	require.NoError(t, err)

	assert.NotEmpty(t, semTable.outerScope.inner)
}
*/
func parseAndAnalyze(t *testing.T, query string) (sqlparser.Statement, *SemTable) {
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse)
	require.NoError(t, err)
	return parse, semTable
}
