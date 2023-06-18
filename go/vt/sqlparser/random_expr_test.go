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

package sqlparser

import (
	"fmt"
	"testing"
	"time"
)

func TestRandomExprWithTables(t *testing.T) {
	schemaTables := []TableT{
		{Name: NewTableName("emp")},
		{Name: NewTableName("dept")},
	}
	schemaTables[0].AddColumns([]Col{
		{Name: "empno", Typ: "bigint"},
		{Name: "ename", Typ: "varchar"},
		{Name: "job", Typ: "varchar"},
		{Name: "mgr", Typ: "bigint"},
		{Name: "hiredate", Typ: "date"},
		{Name: "sal", Typ: "bigint"},
		{Name: "comm", Typ: "bigint"},
		{Name: "deptno", Typ: "bigint"},
	}...)
	schemaTables[1].AddColumns([]Col{
		{Name: "deptno", Typ: "bigint"},
		{Name: "dname", Typ: "varchar"},
		{Name: "loc", Typ: "varchar"},
	}...)

	seed := time.Now().UnixNano()
	g := NewGenerator(seed, 3, schemaTables...)
	randomExpr, _ := g.Expression()
	fmt.Println(String(randomExpr))
}
