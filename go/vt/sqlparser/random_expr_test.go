package sqlparser

import (
	"fmt"
	"golang.org/x/exp/maps"
	"testing"
	"time"
)

func TestRandomExprWithTables(t *testing.T) {
	schema := map[string]TableT{
		"emp": {Name: "emp", Cols: []Col{
			{Name: "empno", Typ: "bigint"},
			{Name: "ename", Typ: "varchar"},
			{Name: "job", Typ: "varchar"},
			{Name: "mgr", Typ: "bigint"},
			{Name: "hiredate", Typ: "date"},
			{Name: "sal", Typ: "bigint"},
			{Name: "comm", Typ: "bigint"},
			{Name: "deptno", Typ: "bigint"},
		}},
		"dept": {Name: "dept", Cols: []Col{
			{Name: "deptno", Typ: "bigint"},
			{Name: "dname", Typ: "varchar"},
			{Name: "loc", Typ: "varchar"},
		}},
	}

	schemaTables := maps.Values(schema)

	seed := time.Now().UnixNano()
	g := NewGenerator(seed, 2, schemaTables...)
	randomExpr := g.Expression()
	fmt.Println(String(randomExpr))
}
