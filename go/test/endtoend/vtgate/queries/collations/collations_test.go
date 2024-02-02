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

package collations

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	// ensure that the vschema and the tables have been created before running any tests
	_ = utils.WaitForAuthoritative(t, keyspaceName, "t1", clusterInstance.VtgateProcess.ReadVSchema)
	_ = utils.WaitForAuthoritative(t, keyspaceName, "t2", clusterInstance.VtgateProcess.ReadVSchema)

	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t2"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestCollationTyping(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")

	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(id, name) values(1,'ß'), (2,'s'), (3,'ss')")
	mcmp.Exec("insert into t2(id, name) values(1,'ß'), (2,'s'), (3,'ss')")

	res := utils.Exec(t, mcmp.VtConn, `vexplain plan SELECT name from t1 union select name from t2`)
	fmt.Printf("%v", res.Rows)
	mcmp.Exec(`SELECT name from t1 union select name from t2`)
}
