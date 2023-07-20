/*
Copyright 2023 The Vitess Authors.

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

package random

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
)

// This test tests that generating random expressions with a schema does not panic
func TestRandomExprWithTables(t *testing.T) {
	//t.Skip("Skip CI")
	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	// specify the schema (that is defined in schema.sql)
	schemaTables := []tableT{
		{tableExpr: sqlparser.NewTableName("emp")},
		{tableExpr: sqlparser.NewTableName("dept")},
	}
	schemaTables[0].addColumns([]column{
		{name: "empno", typ: "bigint"},
		{name: "ename", typ: "varchar"},
		{name: "job", typ: "varchar"},
		{name: "mgr", typ: "bigint"},
		{name: "hiredate", typ: "date"},
		{name: "sal", typ: "bigint"},
		{name: "comm", typ: "bigint"},
		{name: "deptno", typ: "bigint"},
	}...)
	schemaTables[1].addColumns([]column{
		{name: "deptno", typ: "bigint"},
		{name: "dname", typ: "varchar"},
		{name: "loc", typ: "varchar"},
	}...)

	for i := 0; i < 100; i++ {

		seed := time.Now().UnixNano()
		fmt.Printf("seed: %d\n", seed)

		r := rand.New(rand.NewSource(seed))
		genConfig := sqlparser.NewExprGeneratorConfig(sqlparser.CanAggregate, "", 0, false)
		g := sqlparser.NewGenerator(r, 3, slices2.Map(schemaTables, func(t tableT) sqlparser.ExprGenerator { return &t })...)
		expr := g.Expression(genConfig)
		fmt.Println(sqlparser.String(expr))

		from := sqlparser.TableExprs{sqlparser.NewAliasedTableExpr(sqlparser.NewTableName("emp"), ""), sqlparser.NewAliasedTableExpr(sqlparser.NewTableName("dept"), "")}
		query := sqlparser.NewSelect(nil, sqlparser.SelectExprs{sqlparser.NewAliasedExpr(expr, "")}, nil, nil, from, nil, nil, nil, nil)
		mcmp.ExecAllowAndCompareError(sqlparser.String(query))
	}
}
