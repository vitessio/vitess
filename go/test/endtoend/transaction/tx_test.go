/*
Copyright 2019 The Vitess Authors.

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

package transaction

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	hostname        = "localhost"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Reserve vtGate port in order to pass it to vtTablet
		clusterInstance.VtgateGrpcPort = clusterInstance.GetAndReservePort()
		// Set extra tablet args for twopc
		clusterInstance.VtTabletExtraArgs = []string{
			"--twopc_enable",
			"--twopc_abandon_age", "3600",
		}

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:             keyspaceName,
			SchemaSQL:        SchemaSQL,
			VSchema:          VSchema,
			DurabilityPolicy: "semi_sync",
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1, err
		}

		// Starting Vtgate in default MULTI transaction mode
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}
		vtParams = clusterInstance.GetVTParams(keyspaceName)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

// TestTransactionModes tests transactions using twopc mode
func TestTransactionModes(t *testing.T) {
	defer cluster.PanicHandler(t)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set transaction mode to SINGLE.
	utils.Exec(t, conn, "set transaction_mode = 'single'")

	// Insert targeted to multiple tables should fail as Transaction mode is SINGLE
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(1,'john')")
	_, err = conn.ExecuteFetch("insert into twopc_user(user_id, name) values(6,'vick')", 1000, false)
	want := "multi-db transaction attempted"
	require.Error(t, err)
	require.Contains(t, err.Error(), want)
	utils.Exec(t, conn, "rollback")

	// set transaction mode to TWOPC.
	utils.Exec(t, conn, "set transaction_mode = 'twopc'")

	// Insert targeted to multiple db should PASS with TWOPC trx mode
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(3,'mark')")
	utils.Exec(t, conn, "insert into twopc_user(user_id, name) values(4,'doug')")
	utils.Exec(t, conn, "insert into twopc_lookup(name, id) values('Tim',7)")
	utils.Exec(t, conn, "commit")

	// Verify the values are present
	utils.AssertMatches(t, conn, "select user_id from twopc_user where name='mark'", `[[INT64(3)]]`)
	utils.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[[VARCHAR("mark")]]`)

	// DELETE from multiple tables using TWOPC transaction mode
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user where user_id = 3")
	utils.Exec(t, conn, "delete from twopc_lookup where id = 3")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are deleted
	utils.AssertMatches(t, conn, "select user_id from twopc_user where user_id=3", `[]`)
	utils.AssertMatches(t, conn, "select name from twopc_lookup where id=3", `[]`)
}

// TestTransactionIsolation tests transaction isolation level.
func TestTransactionIsolation(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// inserting some data.
	utils.Exec(t, conn, "insert into test(id, msg) values (1,'v1'), (2, 'v2')")
	defer utils.Exec(t, conn, "delete from test")

	conn1, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	// on connection 1 change the isolation level to read-committed.
	// start a transaction and read the data for id = 1.
	utils.Exec(t, conn1, "set transaction isolation level read committed")
	utils.Exec(t, conn1, "begin")
	utils.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("v1")]]`)

	// change the value of msg for id = 1 on connection 2.
	utils.Exec(t, conn2, "update test set msg = 'foo' where id = 1")

	// new value should be reflected on connection 1 within the open transaction.
	utils.AssertMatches(t, conn1, "select id, msg from test where id = 1", `[[INT64(1) VARCHAR("foo")]]`)
	utils.Exec(t, conn1, "rollback")
}

func TestTransactionAccessModes(t *testing.T) {
	closer := start(t)
	defer closer()

	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// start a transaction with read-only characteristic.
	utils.Exec(t, conn, "start transaction read only")
	_, err = utils.ExecAllowError(t, conn, "insert into test(id, msg) values (42,'foo')")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot execute statement in a READ ONLY transaction")
	utils.Exec(t, conn, "rollback")

	// trying autocommit, this should pass as transaction characteristics are limited to single transaction.
	utils.Exec(t, conn, "insert into test(id, msg) values (42,'foo')")

	// target replica
	utils.Exec(t, conn, "use `ks@replica`")
	// start a transaction with read-only characteristic.
	utils.Exec(t, conn, "start transaction read only")
	utils.Exec(t, conn, "select * from test")

	// start a transaction with read-write characteristic. This should fail
	utils.Exec(t, conn, "start transaction read write")
	_, err = utils.ExecAllowError(t, conn, "select connection_id()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot start read write transaction on a read only tablet")
	utils.Exec(t, conn, "rollback")
}

// TestTransactionIsolationInTx tests transaction isolation level inside transaction
// and setting isolation level to different values.
func TestTransactionIsolationInTx(t *testing.T) {
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set transaction isolation level read committed")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	utils.Exec(t, conn, "commit")

	utils.Exec(t, conn, "set transaction isolation level serializable")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("SERIALIZABLE")]]`)
	utils.Exec(t, conn, "commit")

	utils.Exec(t, conn, "set transaction isolation level read committed")
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "select @@transaction_isolation", `[[VARCHAR("READ-COMMITTED")]]`)
	utils.Exec(t, conn, "commit")
}

type commitMetric struct {
	TotalCount  float64
	SingleCount float64
	MultiCount  float64
	TwoPCCount  float64
}

func getCommitMetric(t *testing.T, vtgateProcess cluster.VtgateProcess) commitMetric {
	t.Helper()

	vars, err := clusterInstance.VtgateProcess.GetVars()
	require.NoError(t, err)

	cm := commitMetric{}
	commitVars, exists := vars["CommitModeTimings"]
	if !exists {
		return cm
	}

	commitMap, ok := commitVars.(map[string]any)
	require.True(t, ok, "commit vars is not a map")

	cm.TotalCount = commitMap["TotalCount"].(float64)

	histogram, ok := commitMap["Histograms"].(map[string]any)
	require.True(t, ok, "commit histogram is not a map")

	if single, ok := histogram["Single"]; ok {
		singleMap, ok := single.(map[string]any)
		require.True(t, ok, "single histogram is not a map")
		cm.SingleCount = singleMap["Count"].(float64)
	}

	if multi, ok := histogram["Multi"]; ok {
		multiMap, ok := multi.(map[string]any)
		require.True(t, ok, "multi histogram is not a map")
		cm.MultiCount = multiMap["Count"].(float64)
	}

	if twopc, ok := histogram["TwoPC"]; ok {
		twopcMap, ok := twopc.(map[string]any)
		require.True(t, ok, "twopc histogram is not a map")
		cm.TwoPCCount = twopcMap["Count"].(float64)
	}

	return cm
}

// TestTransactionModes tests transactions using twopc mode
func TestTransactionModeMetrics(t *testing.T) {
	closer := start(t)
	defer closer()

	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	tcases := []struct {
		name  string
		stmts []string
		want  commitMetric
	}{{
		name:  "nothing to commit: so no change on vars",
		stmts: []string{"commit"},
	}, {
		name:  "begin commit - no dml: so no change on vars",
		stmts: []string{"begin", "commit"},
	}, {
		name: "single shard",
		stmts: []string{
			"begin",
			"insert into test(id) values (1)",
			"commit",
		},
		want: commitMetric{TotalCount: 1, SingleCount: 1},
	}, {
		name: "multi shard insert",
		stmts: []string{
			"begin",
			"insert into test(id) values (3),(4)",
			"commit",
		},
		want: commitMetric{TotalCount: 1, MultiCount: 1, TwoPCCount: 1},
	}, {
		name: "multi shard delete",
		stmts: []string{
			"begin",
			"delete from test",
			"commit",
		},
		want: commitMetric{TotalCount: 1, MultiCount: 1, TwoPCCount: 1},
	}}

	initial := getCommitMetric(t, clusterInstance.VtgateProcess)
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.stmts {
				utils.Exec(t, conn, stmt)
			}
			updatedMetric := getCommitMetric(t, clusterInstance.VtgateProcess)
			assert.EqualValues(t, tc.want.TotalCount, updatedMetric.TotalCount-initial.TotalCount, "TotalCount")
			assert.EqualValues(t, tc.want.SingleCount, updatedMetric.SingleCount-initial.SingleCount, "SingleCount")
			assert.EqualValues(t, tc.want.MultiCount, updatedMetric.MultiCount-initial.MultiCount, "MultiCount")
			assert.Zero(t, updatedMetric.TwoPCCount-initial.TwoPCCount, "TwoPCCount")
			initial = updatedMetric
		})
	}

	utils.Exec(t, conn, "set transaction_mode = twopc")
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			for _, stmt := range tc.stmts {
				utils.Exec(t, conn, stmt)
			}
			updatedMetric := getCommitMetric(t, clusterInstance.VtgateProcess)
			assert.EqualValues(t, tc.want.TotalCount, updatedMetric.TotalCount-initial.TotalCount, "TotalCount")
			assert.EqualValues(t, tc.want.SingleCount, updatedMetric.SingleCount-initial.SingleCount, "SingleCount")
			assert.Zero(t, updatedMetric.MultiCount-initial.MultiCount, "MultiCount")
			assert.EqualValues(t, tc.want.TwoPCCount, updatedMetric.TwoPCCount-initial.TwoPCCount, "TwoPCCount")
			initial = updatedMetric
		})
	}
}

func start(t *testing.T) func() {
	deleteAll := func() {
		conn, err := mysql.Connect(context.Background(), &vtParams)
		require.NoError(t, err)
		tables := []string{"test", "twopc_user"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, conn, "delete from "+table)
		}
		conn.Close()
	}

	deleteAll()

	return func() {
		deleteAll()
		cluster.PanicHandler(t)
	}
}
