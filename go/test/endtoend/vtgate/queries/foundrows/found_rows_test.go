/*
Copyright 2021 The Vitess Authors.

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

package foundrows

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestFoundRows(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer vtConn.Close()

	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.Nil(t, err)
	defer mysqlConn.Close()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "delete from t2")
	defer func() {
		utils.Exec(t, vtConn, "set workload = oltp")
		utils.ExecCompareMySQL(t, vtConn, mysqlConn, "delete from t2")

		// queries against lookup tables do not need to be executed against MySQL
		utils.Exec(t, vtConn, "delete from t2_id4_idx")
	}()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t2(id3,id4) values(1,2), (2,2), (3,3), (4,3), (5,3)")

	runTests := func(workload string) {
		// TODO (@frouioui): following assertions produce different results between MySQL and Vitess
		//  their differences are ignored for now. Fix it.
		// `select found_rows()` returns an `UINT64` on Vitess, and `INT64` MySQL
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select * from t2", workload, 5, utils.IgnoreDifference)
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select * from t2 order by id3 limit 2", workload, 2, utils.IgnoreDifference)
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select SQL_CALC_FOUND_ROWS * from t2 order by id3 limit 2", workload, 5, utils.IgnoreDifference)
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select SQL_CALC_FOUND_ROWS * from t2 where id3 = 4 order by id3 limit 2", workload, 1, utils.IgnoreDifference)
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select SQL_CALC_FOUND_ROWS * from t2 where id4 = 3 order by id3 limit 2", workload, 3, utils.IgnoreDifference)
		utils.AssertFoundRowsValueCompareMySQLWithOptions(t, vtConn, mysqlConn, "select SQL_CALC_FOUND_ROWS id4, count(id3) from t2 where id3 = 3 group by id4 limit 1", workload, 1, utils.IgnoreDifference)
	}

	runTests("oltp")
	utils.Exec(t, vtConn, "set workload = olap")
	runTests("olap")
}
