/*
Copyright 2024 The Vitess Authors.

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

package stress

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

// idVals are the primary key values to use while creating insert queries that ensures all the three shards get an insert.
var idVals = [3]int{
	4, // 4 maps to 0x20 and ends up in the first shard (-40)
	6, // 6 maps to 0x60 and ends up in the second shard (40-80)
	9, // 9 maps to 0x90 and ends up in the third shard (80-)
}

func TestSettings(t *testing.T) {
	setup(t)

	testcases := []struct {
		name            string
		commitDelayTime string
		queries         []string
		verifyFunc      func(t *testing.T, vtParams *mysql.ConnParams)
	}{
		{
			name:            "No settings changes",
			commitDelayTime: "5",
			queries:         append([]string{"begin"}, getMultiShardInsertQueries()...),
			verifyFunc: func(t *testing.T, vtParams *mysql.ConnParams) {
				// There is nothing to verify.
			},
		},
		{
			name:            "Settings changes before begin",
			commitDelayTime: "5",
			queries: append(
				append([]string{`set @@time_zone="+10:30"`, "begin"}, getMultiShardInsertQueries()...),
				"insert into twopc_settings(id, col) values(9, now())",
			),
			verifyFunc: func(t *testing.T, vtParams *mysql.ConnParams) {
				// We can check that the time_zone setting was taken into account by checking the diff with the time by using a different time_zone.
				ctx := t.Context()
				conn, err := mysql.Connect(ctx, vtParams)
				require.NoError(t, err)
				defer conn.Close()
				utils.Exec(t, conn, `set @@time_zone="+7:00"`)
				utils.AssertMatches(t, conn, `select HOUR(TIMEDIFF((select col from twopc_settings where id = 9),now()))`, `[[INT64(3)]]`)
			},
		},
		{
			name:            "Settings changes during transaction",
			commitDelayTime: "5",
			queries: append(
				append([]string{"begin"}, getMultiShardInsertQueries()...),
				`set @@time_zone="+10:30"`,
				"insert into twopc_settings(id, col) values(9, now())",
			),
			verifyFunc: func(t *testing.T, vtParams *mysql.ConnParams) {
				// We can check that the time_zone setting was taken into account by checking the diff with the time by using a different time_zone.
				ctx := t.Context()
				conn, err := mysql.Connect(ctx, vtParams)
				require.NoError(t, err)
				defer conn.Close()
				utils.Exec(t, conn, `set @@time_zone="+7:00"`)
				utils.AssertMatches(t, conn, `select HOUR(TIMEDIFF((select col from twopc_settings where id = 9),now()))`, `[[INT64(3)]]`)
			},
		},
		{
			name:            "Settings changes before begin and during transaction",
			commitDelayTime: "5",
			queries: append(
				append([]string{`set @@time_zone="+10:30"`, "begin"}, getMultiShardInsertQueries()...),
				"insert into twopc_settings(id, col) values(9, now())",
				`set @@time_zone="+7:00"`,
				"insert into twopc_settings(id, col) values(25, now())",
			),
			verifyFunc: func(t *testing.T, vtParams *mysql.ConnParams) {
				// We can check that the time_zone setting was taken into account by checking the diff with the time by using a different time_zone.
				ctx := t.Context()
				conn, err := mysql.Connect(ctx, vtParams)
				require.NoError(t, err)
				defer conn.Close()
				utils.AssertMatches(t, conn, `select HOUR(TIMEDIFF((select col from twopc_settings where id = 9),(select col from twopc_settings where id = 25)))`, `[[INT64(3)]]`)
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			// Reparent all the shards to first tablet being the primary.
			reparentToFirstTablet(t)
			// cleanup all the old data.
			conn, closer := start(t)
			defer closer()
			defer twopcutil.DeleteFileFromTablets(t, clusterInstance, keyspaceName, twopcutil.DebugDelayCommitShard)
			defer twopcutil.DeleteFileFromTablets(t, clusterInstance, keyspaceName, twopcutil.DebugDelayCommitTime)
			var wg sync.WaitGroup
			twopcutil.RunMultiShardCommitWithDelayOnTablets(t, clusterInstance, keyspaceName, conn, tt.commitDelayTime, &wg, tt.queries)
			// Allow enough time for the commit to have started.
			time.Sleep(1 * time.Second)
			// Run the vttablet restart to ensure that the transaction needs to be redone.
			err := vttabletRestartShard3(t)
			require.NoError(t, err)
			// Wait for the commit to have returned. We don't actually check for an error in the commit because the user might receive an error.
			// But since we are waiting in CommitPrepared, the decision to commit the transaction should have already been taken.
			wg.Wait()
			// Wair for the data in the table to see that the transaction was committed.
			twopcutil.WaitForResults(t, &vtParams, "select id, col from twopc_t1 where col = 4 order by id", `[[INT64(4) INT64(4)] [INT64(6) INT64(4)] [INT64(9) INT64(4)]]`, 30*time.Second)
			tt.verifyFunc(t, &vtParams)
		})
	}
}

// TestDisruptions tests that atomic transactions persevere through various disruptions.
func TestDisruptions(t *testing.T) {
	setup(t)

	testcases := []struct {
		disruptionName  string
		commitDelayTime string
		setupFunc       func(t *testing.T)
		disruption      func(t *testing.T) error
		resetFunc       func(t *testing.T)
	}{
		{
			disruptionName:  "No Disruption",
			commitDelayTime: "1",
			disruption: func(t *testing.T) error {
				return nil
			},
		},
		{
			disruptionName:  "Resharding",
			commitDelayTime: "20",
			setupFunc:       createShard,
			disruption:      mergeShards,
			resetFunc:       splitShardsBack,
		},
		{
			disruptionName:  "PlannedReparentShard",
			commitDelayTime: "5",
			disruption:      prsShard3,
		},
		{
			disruptionName:  "MySQL Restart",
			commitDelayTime: "5",
			disruption:      mysqlRestartShard3,
		},
		{
			disruptionName:  "Vttablet Restart",
			commitDelayTime: "5",
			disruption:      vttabletRestartShard3,
		},
		{
			disruptionName:  "OnlineDDL",
			commitDelayTime: "20",
			disruption:      onlineDDL,
		},
		{
			disruptionName:  "MoveTables - Complete",
			commitDelayTime: "10",
			disruption:      moveTablesComplete,
			resetFunc:       moveTablesReset,
		},
		{
			disruptionName:  "MoveTables - Cancel",
			commitDelayTime: "10",
			disruption:      moveTablesCancel,
		},
		{
			disruptionName:  "EmergencyReparentShard",
			commitDelayTime: "5",
			disruption:      ersShard3,
		},
	}
	for _, tt := range testcases {
		t.Run(fmt.Sprintf("%s-%ss delay", tt.disruptionName, tt.commitDelayTime), func(t *testing.T) {
			// Reparent all the shards to first tablet being the primary.
			reparentToFirstTablet(t)
			if tt.setupFunc != nil {
				tt.setupFunc(t)
			}
			// cleanup all the old data.
			conn, closer := start(t)
			defer closer()
			defer twopcutil.DeleteFileFromTablets(t, clusterInstance, keyspaceName, twopcutil.DebugDelayCommitShard)
			defer twopcutil.DeleteFileFromTablets(t, clusterInstance, keyspaceName, twopcutil.DebugDelayCommitTime)
			var wg sync.WaitGroup
			twopcutil.RunMultiShardCommitWithDelayOnTablets(t, clusterInstance, keyspaceName, conn, tt.commitDelayTime, &wg, append([]string{"begin"}, getMultiShardInsertQueries()...))
			// Allow enough time for the commit to have started.
			time.Sleep(1 * time.Second)
			writeCtx, writeCancel := context.WithCancel(t.Context())
			var writerWg sync.WaitGroup
			// Run multiple threads to try to write to the database on the same values of id to ensure that we don't
			// allow any writes while the transaction is prepared and not committed.
			for i := range 10 {
				writerWg.Go(func() {
					threadToWrite(t, writeCtx, idVals[i%3])
				})
			}
			// Run the disruption.
			err := tt.disruption(t)
			require.NoError(t, err)
			// Wait for the commit to have returned. We don't actually check for an error in the commit because the user might receive an error.
			// But since we are waiting in CommitPrepared, the decision to commit the transaction should have already been taken.
			wg.Wait()
			// Check the data in the table.
			twopcutil.WaitForResults(t, &vtParams, "select id, col from twopc_t1 where col = 4 order by id", `[[INT64(4) INT64(4)] [INT64(6) INT64(4)] [INT64(9) INT64(4)]]`, 30*time.Second)
			writeCancel()
			writerWg.Wait()

			if tt.resetFunc != nil {
				tt.resetFunc(t)
			}
		})
	}
}

// getMultiShardInsertQueries gets the queries that will cause one insert on all the shards.
func getMultiShardInsertQueries() []string {
	var queries []string
	// Insert rows such that they go to all the three shards. Given that we have sharded the table `twopc_t1` on reverse_bits
	// it is very easy to figure out what value will end up in which shard.
	for _, val := range idVals {
		queries = append(queries, fmt.Sprintf("insert into twopc_t1(id, col) values(%d, 4)", val))
	}
	return queries
}

func mergeShards(t *testing.T) error {
	return twopcutil.RunReshardWorkflow(t, clusterInstance, "TestDisruptions", keyspaceName, "40-80,80-", "40-")
}

func splitShardsBack(t *testing.T) {
	t.Helper()
	twopcutil.AddShardsToKeyspace(t, clusterInstance, keyspaceName, []string{"40-80", "80-"})
	err := twopcutil.RunReshardWorkflow(t, clusterInstance, "TestDisruptions", keyspaceName, "40-", "40-80,80-")
	require.NoError(t, err)
}

// createShard creates a new shard in the keyspace that we'll use for Resharding.
func createShard(t *testing.T) {
	t.Helper()
	twopcutil.AddShardsToKeyspace(t, clusterInstance, keyspaceName, []string{"40-"})
}

// threadToWrite is a helper function to write to the database in a loop.
func threadToWrite(t *testing.T, ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		conn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			continue
		}
		_, _ = utils.ExecAllowError(t, conn, fmt.Sprintf("insert into twopc_t1(id, col) values(%d, %d)", id, rand.IntN(10000)))
		conn.Close()
	}
}

// reparentToFirstTablet reparents all the shards to first tablet being the primary.
func reparentToFirstTablet(t *testing.T) {
	ctx := t.Context()
	for _, shard := range clusterInstance.Keyspace(keyspaceName).Shards() {
		primary := shard.Tablets()[0]
		err := clusterInstance.Vtctld().ExecuteCommand(ctx, "PlannedReparentShard", shard.Ref(), "--new-primary", primary.Alias())
		require.NoError(t, err)
	}
}

/*
Cluster Level Disruptions for the fuzzer
*/

// prsShard3 runs a PRS in shard 3 of the keyspace. It promotes the second tablet to be the new primary.
func prsShard3(t *testing.T) error {
	shard := clusterInstance.Keyspace(keyspaceName).Shards()[2]
	newPrimary := shard.Tablets()[1]
	return clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard", shard.Ref(), "--new-primary", newPrimary.Alias())
}

// ersShard3 runs a ERS in shard 3 of the keyspace. It promotes the second tablet to be the new primary.
func ersShard3(t *testing.T) error {
	shard := clusterInstance.Keyspace(keyspaceName).Shards()[2]
	newPrimary := shard.Tablets()[1]
	_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "EmergencyReparentShard", fmt.Sprintf("%s/%s", keyspaceName, shard.Name), "--new-primary", newPrimary.Alias())
	return err
}

// vttabletRestartShard3 restarts the first vttablet of the third shard.
func vttabletRestartShard3(t *testing.T) error {
	ctx := t.Context()
	shard := clusterInstance.Keyspace(keyspaceName).Shards()[2]
	tablet := shard.Tablets()[0]
	_ = tablet.StopVttablet(ctx)
	return tablet.StartVttablet(ctx)
}

// mysqlRestartShard3 restarts MySQL on the first tablet of the third shard.
func mysqlRestartShard3(t *testing.T) error {
	ctx := t.Context()
	shard := clusterInstance.Keyspace(keyspaceName).Shards()[2]
	vttablets := shard.Tablets()
	tablet := vttablets[0]
	log.Error(fmt.Sprintf("Restarting MySQL for - %v/%v tablet - %v", keyspaceName, shard.Name, tablet.Alias()))
	if err := tablet.KillMySQL(ctx); err != nil {
		return err
	}
	return tablet.StartMySQL(ctx)
}

// moveTablesCancel runs a move tables command that we cancel in the end.
func moveTablesCancel(t *testing.T) error {
	ctx := t.Context()
	workflow := "TestDisruptions"
	moveTables := func(args ...string) (string, error) {
		cmd := append([]string{"MoveTables", "--workflow=" + workflow, "--target-keyspace=" + unshardedKeyspaceName}, args...)
		return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, cmd...)
	}
	// Initiate MoveTables for twopc_t1.
	output, err := moveTables("Create", "--source-keyspace="+keyspaceName, "--tables=twopc_t1",
		"--tablet-types", topodatapb.TabletType_REPLICA.String())
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	twopcutil.WaitForVreplCatchup(t, clusterInstance.Keyspace(unshardedKeyspaceName).Shards(), workflow, 10*time.Second)
	// SwitchTraffic
	output, err = moveTables("SwitchTraffic")
	require.NoError(t, err, output)
	output, err = moveTables("ReverseTraffic")
	require.NoError(t, err, output)
	output, err = moveTables("Cancel")
	require.NoError(t, err, output)
	return nil
}

// moveTablesComplete runs a move tables command that we complete in the end.
func moveTablesComplete(t *testing.T) error {
	ctx := t.Context()
	workflow := "TestDisruptions"
	moveTables := func(args ...string) (string, error) {
		cmd := append([]string{"MoveTables", "--workflow=" + workflow, "--target-keyspace=" + unshardedKeyspaceName}, args...)
		return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, cmd...)
	}
	// Initiate MoveTables for twopc_t1.
	output, err := moveTables("Create", "--source-keyspace="+keyspaceName, "--tables=twopc_t1",
		"--tablet-types", topodatapb.TabletType_REPLICA.String())
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	twopcutil.WaitForVreplCatchup(t, clusterInstance.Keyspace(unshardedKeyspaceName).Shards(), workflow, 10*time.Second)
	// SwitchTraffic
	output, err = moveTables("SwitchTraffic")
	require.NoError(t, err, output)
	output, err = moveTables("Complete")
	require.NoError(t, err, output)
	return nil
}

// moveTablesReset moves the table back from the unsharded keyspace to sharded
func moveTablesReset(t *testing.T) {
	ctx := t.Context()
	// We apply the vschema again because previous move tables would have removed the entry for `twopc_t1`.
	err := clusterInstance.Vtctld().ExecuteCommand(ctx, "ApplyVSchema", "--vschema", VSchema, keyspaceName)
	require.NoError(t, err)
	workflow := "TestDisruptions"
	moveTables := func(args ...string) (string, error) {
		cmd := append([]string{"MoveTables", "--workflow=" + workflow, "--target-keyspace=" + keyspaceName}, args...)
		return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, cmd...)
	}
	// Initiate MoveTables for twopc_t1.
	output, err := moveTables("Create", "--source-keyspace="+unshardedKeyspaceName, "--tables=twopc_t1",
		"--tablet-types", topodatapb.TabletType_REPLICA.String())
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	twopcutil.WaitForVreplCatchup(t, clusterInstance.Keyspace(keyspaceName).Shards(), workflow, 10*time.Second)
	// SwitchTraffic
	output, err = moveTables("SwitchTraffic")
	require.NoError(t, err, output)
	output, err = moveTables("Complete")
	require.NoError(t, err, output)
}

var orderedDDL = []string{
	"alter table twopc_t1 add column extra_col1 varchar(20)",
	"alter table twopc_t1 add column extra_col2 varchar(20)",
	"alter table twopc_t1 add column extra_col3 varchar(20)",
	"alter table twopc_t1 add column extra_col4 varchar(20)",
}

var count = 0

// onlineDDL runs a DDL statement.
func onlineDDL(t *testing.T) error {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema",
		"--sql", orderedDDL[count%len(orderedDDL)],
		"--ddl-strategy", "vitess --force-cut-over-after=1ms",
		keyspaceName)
	require.NoError(t, err)
	count++
	fmt.Println("uuid: ", output)
	shards := clusterInstance.Keyspace(keyspaceName).Shards()
	status := twopcutil.WaitForMigrationStatusOnShards(t, &vtParams, keyspaceName, shards,
		strings.TrimSpace(output), 2*time.Minute, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	checkMigrationStatus(t, &vtParams, shards, strings.TrimSpace(output), status)
	require.Equal(t, schema.OnlineDDLStatusComplete, status)
	return nil
}

// checkMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func checkMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	ksName := shards[0].Keyspace.Name
	query, err := sqlparser.ParseAndBind(
		fmt.Sprintf("show vitess_migrations from %s like %%a", ksName),
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)
	onlineddl.PrintQueryResult(os.Stdout, r)

	count := 0
	for _, row := range r.Named().Rows {
		if row["migration_uuid"].ToString() != uuid {
			continue
		}
		for _, expectStatus := range expectStatuses {
			if row["migration_status"].ToString() == string(expectStatus) {
				count++
				break
			}
		}
	}
	return assert.Equal(t, len(shards), count)
}
