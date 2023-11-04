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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestFoundRows(t *testing.T) {
	defer cluster.PanicHandler(t)
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	defer mcmp.Close()

	_, _ = mcmp.ExecAndIgnore("delete from t2")
	defer func() {
		utils.Exec(t, mcmp.VtConn, "set workload = oltp")
		_, _ = mcmp.ExecAndIgnore("delete from t2")
		// queries against lookup tables do not need to be executed against MySQL
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "delete from t2_id4_idx")
	}()

	mcmp.Exec("insert into t2(id3,id4) values(1,2), (2,2), (3,3), (4,3), (5,3)")

	// Wait for schema tracking to run and mark t2 as authoritative before we try out the queries.
	// Some of the queries depend on schema tracking to run successfully to be able to replace the StarExpr
	// in the select clause with the definitive column list.
	err = utils.WaitForAuthoritative(t, keyspaceName, "t2", clusterInstance.VtgateProcess.ReadVSchema)
	require.NoError(t, err)
	runTests := func(workload string) {
		mcmp.AssertFoundRowsValue("select * from t2", workload, 5)
		mcmp.AssertFoundRowsValue("select * from t2 order by id3 limit 2", workload, 2)
		mcmp.AssertFoundRowsValue("select SQL_CALC_FOUND_ROWS * from t2 order by id3 limit 2", workload, 5)
		mcmp.AssertFoundRowsValue("select SQL_CALC_FOUND_ROWS * from t2 where id3 = 4 order by id3 limit 2", workload, 1)
		mcmp.AssertFoundRowsValue("select SQL_CALC_FOUND_ROWS * from t2 where id4 = 3 order by id3 limit 2", workload, 3)
		mcmp.AssertFoundRowsValue("select SQL_CALC_FOUND_ROWS id4, count(id3) from t2 where id3 = 3 group by id4 limit 1", workload, 1)
	}

	runTests("oltp")
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	runTests("olap")
}
