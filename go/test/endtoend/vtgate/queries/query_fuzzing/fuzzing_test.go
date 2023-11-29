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

package query_fuzzing

import (
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
)

// this test uses the AST defined in the sqlparser package to randomly generate queries

// if true then execution will always stop on a "must fix" error: a results mismatched or EOF
const stopOnMustFixError = false

//go:embed emp_insert.sql
var empInsert string

//go:embed dept_insert.sql
var deptInsert string

func helperTest(t *testing.T, query string) {
	t.Helper()
	t.Run(query, func(t *testing.T) {
		mcmp, closer := start(t)
		defer closer()

		result, err := mcmp.ExecAllowAndCompareError(query)
		fmt.Println(result)
		fmt.Println(err)
	})
}

func TestFuzzQueries(t *testing.T) {
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

	endBy := time.Now().Add(1 * time.Second)

	var queryCount, queryFailCount int
	// continue testing after an error if and only if testFailingQueries is true
	for time.Now().Before(endBy) && (!t.Failed() || !testFailingQueries) {
		seed := time.Now().UnixNano()
		// genConfig := sqlparser.NewExprGeneratorConfig(sqlparser.CannotAggregate, "", 0, false)
		// qg := newQueryGenerator(rand.New(rand.NewSource(seed)), genConfig, 2, 2, 2, schemaTables)
		qg := &maruts{}
		query := sqlparser.String(qg.randomQuery())
		t.Run(query, func(t *testing.T) {
			_, vtErr := mcmp.ExecAllowAndCompareError(query)

			// this assumes all queries are valid mysql queries
			if vtErr != nil {
				fmt.Printf("seed: %d\n", seed)
				fmt.Println(query)
				fmt.Println(vtErr)

				if stopOnMustFixError {
					// results mismatched
					if strings.Contains(vtErr.Error(), "results mismatched") {
						simplified := simplifyResultsMismatchedQuery(t, query)
						fmt.Printf("final simplified query: %s\n", simplified)
						t.Fatal("failed")
					}
					// EOF
					var sqlError *sqlerror.SQLError
					if errors.As(vtErr, &sqlError) && strings.Contains(sqlError.Message, "EOF") {
						t.Fatal("failed")
					}
				}

				// restart the mysql and vitess connections in case something bad happened
				closer()
				mcmp, closer = start(t)

				fmt.Printf("\n\n\n")
				queryFailCount++
			}
			queryCount++
		})
	}
	fmt.Printf("Queries successfully executed: %d\n", queryCount)
	fmt.Printf("Queries failed: %d\n", queryFailCount)
}
