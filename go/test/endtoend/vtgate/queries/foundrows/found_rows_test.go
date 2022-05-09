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
	"fmt"
	"testing"
	"time"

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
	err = waitForT2Authoritative(t)
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

// waitForT2Authoritative waits for the table t2 to become authoritative
func waitForT2Authoritative(t *testing.T) error {
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("schema tracking didn't mark table t2 as authoritative until timeout")
		default:
			time.Sleep(1 * time.Second)
			res, err := clusterInstance.VtgateProcess.ReadVSchema()
			require.NoError(t, err, res)
			t2Map := getTableT2Map(res)
			authoritative, fieldPresent := t2Map["column_list_authoritative"]
			if !fieldPresent {
				continue
			}
			authoritativeBool, isBool := authoritative.(bool)
			if !isBool || !authoritativeBool {
				continue
			}
			return nil
		}
	}
}

func getTableT2Map(res *interface{}) map[string]interface{} {
	t2Map := convertToMap(convertToMap(convertToMap(convertToMap(convertToMap(*res)["keyspaces"])[keyspaceName])["tables"])["t2"])
	return t2Map
}

func convertToMap(input interface{}) map[string]interface{} {
	output := input.(map[string]interface{})
	return output
}
