package main

import (
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
)

func main() {

	sqls := []string{
		"SELECT col1, col2 FROM aTable /*vt+ CRITICALITY=20 SCATTER_ERRORS_AS_WARNINGS something=other abc=def WORKLOAD_NAME=thing; */",
		"SELECT col1, col2 /*vt+ CRITICALITY=20 SCATTER_ERRORS_AS_WARNINGS something=other abc=def WORKLOAD_NAME=thing; */ FROM aTable ",
		"SELECT /*vt+ CRITICALITY=20 SCATTER_ERRORS_AS_WARNINGS something=other abc=def WORKLOAD_NAME=thing; */ col1, col2 FROM aTable ",
		"UPDATE the_table /*vt+ CRITICALITY=20 SCATTER_ERRORS_AS_WARNINGS something=other abc=def WORKLOAD_NAME=thing; */ SET a=1, b=2",
		"UPDATE /*vt+ CRITICALITY=20 SCATTER_ERRORS_AS_WARNINGS something=other abc=def WORKLOAD_NAME=thing; */ the_table SET a=1, b=2",
	}

	for _, sql := range sqls {
		stmt, _, err := sqlparser.Parse2(sql)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Directives: %+v\n", sqlparser.GetWorkloadNameFromStatement(stmt))
	}
}
