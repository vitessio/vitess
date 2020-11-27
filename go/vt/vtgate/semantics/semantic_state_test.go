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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSimpleColumn(t *testing.T) {
	query := "select col from t"
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse)
	require.NoError(t, err)

	sel := parse.(*sqlparser.Select)
	columnExpr := sel.SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)

	deps, err := semTable.DependenciesFor(columnExpr)
	require.NoError(t, err)

	require.Equal(t, 1, len(deps))
	dependency := deps[0]
	assert.IsType(t, &TableExpression{}, dependency)
	expression := dependency.(*TableExpression)
	expr := expression.te.(*sqlparser.AliasedTableExpr)
	tableName := expr.Expr.(sqlparser.TableName)
	name := tableName.Name
	assert.Equal(t, "apa", name)
}

func TestScoping(t *testing.T) {
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
