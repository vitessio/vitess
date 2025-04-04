/*
Copyright 2025 The Vitess Authors.

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

package multi_query

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestMultiQuery tests the new way of handling queries in vtgate
// that runs multiple queries together.
func TestMultiQuery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	validSqlQueries := []string{
		"select * from t1",         // Single route query
		"select * from t1 join t2", // Join query
		"select * from t1 join t2 on t1.id1 = t2.id5 where t1.id1 = 4",                                                                     // Join query that can be pushed down
		"select * from t1; select * from t2; select * from t1 join t2;",                                                                    // Multiple select queries
		"select * from t1; insert into t2(id5, id6, id7) values (40, 43, 46); select * from t2; delete from t2; select * from t1 join t2;", // Multiple queries with dml in between
	}

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf(`set workload = %s`, workload))
			defer utils.Exec(t, mcmp.VtConn, `set workload = oltp`)

			for _, query := range validSqlQueries {
				mcmp.ExecMulti(query)
			}
		})
	}
}
