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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestSeed makes sure that the seed is deterministic
func TestSeed(t *testing.T) {
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

	seed := int64(1689757943775102000)
	genConfig := sqlparser.NewExprGeneratorConfig(sqlparser.CannotAggregate, "", 0, false)
	qg := newQueryGenerator(rand.New(rand.NewSource(seed)), genConfig, 2, 2, 2, schemaTables)
	qg.randomQuery()
	query1 := sqlparser.String(qg.stmt)
	qg = newQueryGenerator(rand.New(rand.NewSource(seed)), genConfig, 2, 2, 2, schemaTables)
	qg.randomQuery()
	query2 := sqlparser.String(qg.stmt)
	fmt.Println(query1)
	require.Equal(t, query1, query2)
}
