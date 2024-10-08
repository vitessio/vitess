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
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/syscallutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	twopcutil "vitess.io/vitess/go/test/endtoend/transaction/twopc/utils"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
)

// TestDisruptions tests that atomic transactions persevere through various disruptions.
func TestDisruptions(t *testing.T) {
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
			// Start an atomic transaction.
			utils.Exec(t, conn, "begin")
			// Insert rows such that they go to all the three shards. Given that we have sharded the table `twopc_t1` on reverse_bits
			// it is very easy to figure out what value will end up in which shard.
			idVals := []int{4, 6, 9}
			for _, val := range idVals {
				utils.Exec(t, conn, fmt.Sprintf("insert into twopc_t1(id, col) values(%d, 4)", val))
			}
			// We want to delay the commit on one of the shards to simulate slow commits on a shard.
			twopcutil.WriteTestCommunicationFile(t, twopcutil.DebugDelayCommitShard, "80-")
			defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitShard)
			twopcutil.WriteTestCommunicationFile(t, twopcutil.DebugDelayCommitTime, tt.commitDelayTime)
			defer twopcutil.DeleteFile(twopcutil.DebugDelayCommitTime)
			// We will execute a commit in a go routine, because we know it will take some time to complete.
			// While the commit is ongoing, we would like to run the disruption.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := utils.ExecAllowError(t, conn, "commit")
				if err != nil {
					log.Errorf("Error in commit - %v", err)
				}
			}()
			// Allow enough time for the commit to have started.
			time.Sleep(1 * time.Second)
			writeCtx, writeCancel := context.WithCancel(context.Background())
			var writerWg sync.WaitGroup
			// Run multiple threads to try to write to the database on the same values of id to ensure that we don't
			// allow any writes while the transaction is prepared and not committed.
			for i := 0; i < 10; i++ {
				writerWg.Add(1)
				go func() {
					defer writerWg.Done()
					threadToWrite(t, writeCtx, idVals[i%3])
				}()
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

func mergeShards(t *testing.T) error {
	return twopcutil.RunReshard(t, clusterInstance, "TestDisruptions", keyspaceName, "40-80,80-", "40-")
}

func splitShardsBack(t *testing.T) {
	t.Helper()
	twopcutil.AddShards(t, clusterInstance, keyspaceName, []string{"40-80", "80-"})
	err := twopcutil.RunReshard(t, clusterInstance, "TestDisruptions", keyspaceName, "40-", "40-80,80-")
	require.NoError(t, err)
}

// createShard creates a new shard in the keyspace that we'll use for Resharding.
func createShard(t *testing.T) {
	t.Helper()
	twopcutil.AddShards(t, clusterInstance, keyspaceName, []string{"40-"})
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
		_, _ = utils.ExecAllowError(t, conn, fmt.Sprintf("insert into twopc_t1(id, col) values(%d, %d)", id, rand.Intn(10000)))
		conn.Close()
	}
}

// reparentToFirstTablet reparents all the shards to first tablet being the primary.
func reparentToFirstTablet(t *testing.T) {
	ks := clusterInstance.Keyspaces[0]
	for _, shard := range ks.Shards {
		primary := shard.Vttablets[0]
		err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, primary.Alias)
		require.NoError(t, err)
	}
}

/*
Cluster Level Disruptions for the fuzzer
*/

// prsShard3 runs a PRS in shard 3 of the keyspace. It promotes the second tablet to be the new primary.
func prsShard3(t *testing.T) error {
	shard := clusterInstance.Keyspaces[0].Shards[2]
	newPrimary := shard.Vttablets[1]
	return clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, newPrimary.Alias)
}

// ersShard3 runs a ERS in shard 3 of the keyspace. It promotes the second tablet to be the new primary.
func ersShard3(t *testing.T) error {
	shard := clusterInstance.Keyspaces[0].Shards[2]
	newPrimary := shard.Vttablets[1]
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("EmergencyReparentShard", fmt.Sprintf("%s/%s", keyspaceName, shard.Name), "--new-primary", newPrimary.Alias)
	return err
}

// vttabletRestartShard3 restarts the first vttablet of the third shard.
func vttabletRestartShard3(t *testing.T) error {
	shard := clusterInstance.Keyspaces[0].Shards[2]
	tablet := shard.Vttablets[0]
	return tablet.RestartOnlyTablet()
}

// mysqlRestartShard3 restarts MySQL on the first tablet of the third shard.
func mysqlRestartShard3(t *testing.T) error {
	shard := clusterInstance.Keyspaces[0].Shards[2]
	vttablets := shard.Vttablets
	tablet := vttablets[0]
	log.Errorf("Restarting MySQL for - %v/%v tablet - %v", keyspaceName, shard.Name, tablet.Alias)
	pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.pid", tablet.TabletUID))
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		// We can't read the file which means the PID file does not exist
		// The server must have stopped
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		return err
	}
	return syscallutil.Kill(pid, syscall.SIGKILL)
}

// moveTablesCancel runs a move tables command that we cancel in the end.
func moveTablesCancel(t *testing.T) error {
	workflow := "TestDisruptions"
	mtw := cluster.NewMoveTables(t, clusterInstance, workflow, unshardedKeyspaceName, keyspaceName, "twopc_t1", []string{topodatapb.TabletType_REPLICA.String()})
	// Initiate MoveTables for twopc_t1.
	output, err := mtw.Create()
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	mtw.WaitForVreplCatchup(10 * time.Second)
	// SwitchTraffic
	output, err = mtw.SwitchReadsAndWrites()
	require.NoError(t, err, output)
	output, err = mtw.ReverseReadsAndWrites()
	require.NoError(t, err, output)
	output, err = mtw.Cancel()
	require.NoError(t, err, output)
	return nil
}

// moveTablesComplete runs a move tables command that we complete in the end.
func moveTablesComplete(t *testing.T) error {
	workflow := "TestDisruptions"
	mtw := cluster.NewMoveTables(t, clusterInstance, workflow, unshardedKeyspaceName, keyspaceName, "twopc_t1", []string{topodatapb.TabletType_REPLICA.String()})
	// Initiate MoveTables for twopc_t1.
	output, err := mtw.Create()
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	mtw.WaitForVreplCatchup(10 * time.Second)
	// SwitchTraffic
	output, err = mtw.SwitchReadsAndWrites()
	require.NoError(t, err, output)
	output, err = mtw.Complete()
	require.NoError(t, err, output)
	return nil
}

// moveTablesReset moves the table back from the unsharded keyspace to sharded
func moveTablesReset(t *testing.T) {
	// We apply the vschema again because previous move tables would have removed the entry for `twopc_t1`.
	err := clusterInstance.VtctldClientProcess.ApplyVSchema(keyspaceName, VSchema)
	require.NoError(t, err)
	workflow := "TestDisruptions"
	mtw := cluster.NewMoveTables(t, clusterInstance, workflow, keyspaceName, unshardedKeyspaceName, "twopc_t1", []string{topodatapb.TabletType_REPLICA.String()})
	// Initiate MoveTables for twopc_t1.
	output, err := mtw.Create()
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	mtw.WaitForVreplCatchup(10 * time.Second)
	// SwitchTraffic
	output, err = mtw.SwitchReadsAndWrites()
	require.NoError(t, err, output)
	output, err = mtw.Complete()
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
	output, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(keyspaceName, orderedDDL[count%len(orderedDDL)], cluster.ApplySchemaParams{
		DDLStrategy: "vitess --force-cut-over-after=1ms",
	})
	require.NoError(t, err)
	count++
	fmt.Println("uuid: ", output)
	status := twopcutil.WaitForMigrationStatus(t, &vtParams, keyspaceName, clusterInstance.Keyspaces[0].Shards,
		strings.TrimSpace(output), 2*time.Minute, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	onlineddl.CheckMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, strings.TrimSpace(output), status)
	require.Equal(t, schema.OnlineDDLStatusComplete, status)
	return nil
}
