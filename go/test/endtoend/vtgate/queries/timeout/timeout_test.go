/*
Copyright 2022 The Vitess Authors.

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

package misc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"t1", "uks.unsharded"}
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

func TestQueryTimeoutWithDual(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.04) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.24) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "set @@session.query_timeout=20")
	require.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.01) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=500 */ sleep(0.24) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=10 */ sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=15 */ sleep(0.001) from dual")
	assert.NoError(t, err)
}

func TestQueryTimeoutWithTables(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// unsharded
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	for i := 0; i < 12; i++ {
		utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=2000 */ into uks.unsharded(id1) select id1+5 from uks.unsharded")
	}

	utils.Exec(t, mcmp.VtConn, "select count(*) from uks.unsharded where id1 > 31")
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=100 */ count(*) from uks.unsharded where id1 > 31")

	// the query usually takes more than 5ms to return. So this should fail.
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=1 */ count(*) from uks.unsharded where id1 > 31")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Contains(t, err.Error(), "(errno 1317) (sqlstate 70100)")

	// sharded
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into ks_misc.t1(id1, id2) values (1,2),(2,4),(3,6),(4,8),(5,10)")

	// sleep take in seconds, so 0.1 is 100ms
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=500 */ sleep(0.1) from t1 where id1 = 1")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=20 */ sleep(0.1) from t1 where id1 = 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Contains(t, err.Error(), "(errno 1317) (sqlstate 70100)")
}
