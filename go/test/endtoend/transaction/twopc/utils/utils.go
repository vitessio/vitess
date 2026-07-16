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

package utils

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
)

const (
	DebugDelayCommitShard = "VT_DELAY_COMMIT_SHARD"
	DebugDelayCommitTime  = "VT_DELAY_COMMIT_TIME"

	// containerVTDataRoot is the VTDATAROOT path inside tablet containers, where
	// debug-mode vttablets poll for the commit-delay coordination files.
	containerVTDataRoot = "/vt/vtdataroot"
)

// ClearOutTable deletes everything from a table. Sometimes the table might have more rows than allowed in a single delete query,
// so we have to do the deletions iteratively.
func ClearOutTable(t testing.TB, vtParams mysql.ConnParams, tableName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout out waiting for table to be cleared - %v", tableName)
			return
		default:
		}
		conn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			log.Error(fmt.Sprintf("Error in connection - %v\n", err))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT count(*) FROM %v", tableName), 1, false)
		if err != nil {
			log.Error(fmt.Sprintf("Error in selecting - %v\n", err))
			conn.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		require.Len(t, res.Rows, 1)
		require.Len(t, res.Rows[0], 1)
		rowCount, err := res.Rows[0][0].ToInt()
		require.NoError(t, err)
		if rowCount == 0 {
			conn.Close()
			return
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("DELETE FROM %v LIMIT 10000", tableName), 10000, false)
		conn.Close()
		if err != nil {
			log.Error(fmt.Sprintf("Error in cleanup deletion - %v\n", err))
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
}

// WriteTestCommunicationFile writes the content to the file with the given name.
// We use these files to coordinate with the vttablets running in the debug mode.
func WriteTestCommunicationFile(t *testing.T, fileName string, content string) {
	// Delete the file just to make sure it doesn't exist before we write to it.
	DeleteFile(fileName)
	err := os.WriteFile(path.Join(os.Getenv("VTDATAROOT"), fileName), []byte(content), 0o644)
	require.NoError(t, err)
}

// RunMultiShardCommitWithDelay runs a multi shard commit and configures it to wait for a certain amount of time in the commit phase.
func RunMultiShardCommitWithDelay(t *testing.T, conn *mysql.Conn, commitDelayTime string, wg *sync.WaitGroup, queries []string) {
	// Run all the queries to start the transaction.
	for _, query := range queries {
		utils.Exec(t, conn, query)
	}
	// We want to delay the commit on one of the shards to simulate slow commits on a shard.
	WriteTestCommunicationFile(t, DebugDelayCommitShard, "80-")
	WriteTestCommunicationFile(t, DebugDelayCommitTime, commitDelayTime)
	// We will execute a commit in a go routine, because we know it will take some time to complete.
	// While the commit is ongoing, we would like to run the disruption.
	wg.Go(func() {
		_, err := utils.ExecAllowError(t, conn, "commit")
		if err != nil {
			log.Error(fmt.Sprintf("Error in commit - %v", err))
		}
	})
}

// DeleteFile deletes the file specified.
func DeleteFile(fileName string) {
	_ = os.Remove(path.Join(os.Getenv("VTDATAROOT"), fileName))
}

// WriteTestCommunicationFileToTablets writes content to fileName in the data
// directory of every shard primary in the keyspace. Debug-mode vttablets poll
// these files to coordinate commit delays during tests.
func WriteTestCommunicationFileToTablets(t *testing.T, clusterInstance *vitesst.Cluster, keyspace, fileName, content string) {
	// Delete the file just to make sure it doesn't exist before we write to it.
	DeleteFileFromTablets(t, clusterInstance, keyspace, fileName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shard := range clusterInstance.Keyspace(keyspace).Shards() {
		err := shard.Primary().WriteFile(ctx, path.Join(containerVTDataRoot, fileName), content)
		require.NoError(t, err)
	}
}

// DeleteFileFromTablets removes fileName from the data directory of every shard
// primary in the keyspace.
func DeleteFileFromTablets(t *testing.T, clusterInstance *vitesst.Cluster, keyspace, fileName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shard := range clusterInstance.Keyspace(keyspace).Shards() {
		err := shard.Primary().RemoveFile(ctx, path.Join(containerVTDataRoot, fileName))
		require.NoError(t, err)
	}
}

// RunMultiShardCommitWithDelayOnTablets runs a multi shard commit and configures
// the shard primaries to wait for commitDelayTime seconds in the commit phase.
func RunMultiShardCommitWithDelayOnTablets(t *testing.T, clusterInstance *vitesst.Cluster, keyspace string, conn *mysql.Conn, commitDelayTime string, wg *sync.WaitGroup, queries []string) {
	// Run all the queries to start the transaction.
	for _, query := range queries {
		utils.Exec(t, conn, query)
	}
	// We want to delay the commit on one of the shards to simulate slow commits on a shard.
	WriteTestCommunicationFileToTablets(t, clusterInstance, keyspace, DebugDelayCommitShard, "80-")
	WriteTestCommunicationFileToTablets(t, clusterInstance, keyspace, DebugDelayCommitTime, commitDelayTime)
	// We will execute a commit in a go routine, because we know it will take some time to complete.
	// While the commit is ongoing, we would like to run the disruption.
	wg.Go(func() {
		_, err := utils.ExecAllowError(t, conn, "commit")
		if err != nil {
			log.Error(fmt.Sprintf("Error in commit - %v", err))
		}
	})
}

// WaitForResults waits for the results of the query to be as expected.
func WaitForResults(t *testing.T, vtParams *mysql.ConnParams, query string, resultExpected string, waitTime time.Duration) {
	timeout := time.After(waitTime)
	var prevRes []sqltypes.Row
	for {
		select {
		case <-timeout:
			t.Fatalf("didn't reach expected results for %s. Last results - %v", query, prevRes)
		default:
			ctx := t.Context()
			conn, err := mysql.Connect(ctx, vtParams)
			if err == nil {
				res, _ := utils.ExecAllowError(t, conn, query)
				conn.Close()
				if res != nil {
					prevRes = res.Rows
					if fmt.Sprintf("%v", res.Rows) == resultExpected {
						return
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// WaitForMigrationStatusOnShards waits for the migration to reach one of the expected statuses on all the given shards.
func WaitForMigrationStatusOnShards(t *testing.T, vtParams *mysql.ConnParams, ks string, shards []*vitesst.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	names := make([]string, 0, len(shards))
	for _, shard := range shards {
		names = append(names, shard.Name)
	}
	return waitForMigrationStatus(t, vtParams, ks, names, uuid, timeout, expectStatuses...)
}

func waitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, ks string, shards []string, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard] = true
	}
	query := fmt.Sprintf("show vitess_migrations from %s like '%s'", ks, uuid)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastKnownStatus := ""
	for {
		select {
		case <-ctx.Done():
			return schema.OnlineDDLStatus(lastKnownStatus)
		case <-ticker.C:
		}
		countMatchedShards := 0
		conn, err := mysql.Connect(ctx, vtParams)
		if err != nil {
			continue
		}
		r, err := utils.ExecAllowError(t, conn, query)
		conn.Close()
		if err != nil {
			continue
		}
		for _, row := range r.Named().Rows {
			shardName := row["shard"].ToString()
			if !shardNames[shardName] {
				// irrelevant shard
				continue
			}
			lastKnownStatus = row["migration_status"].ToString()
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == len(shards) {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
	}
}

// RunReshardWorkflow reshards the keyspace from sourceShards to targetShards and removes the source shards
// once the workflow completes. The target shards must already exist.
func RunReshardWorkflow(t *testing.T, clusterInstance *vitesst.Cluster, workflowName, keyspaceName, sourceShards, targetShards string) error {
	ctx := t.Context()
	reshard := func(args ...string) (string, error) {
		cmd := append([]string{"Reshard", "--workflow=" + workflowName, "--target-keyspace=" + keyspaceName}, args...)
		return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, cmd...)
	}

	// Initiate Reshard.
	output, err := reshard("Create", "--source-shards="+sourceShards, "--target-shards="+targetShards)
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	WaitForVreplCatchup(t, keyspaceShards(clusterInstance, keyspaceName, strings.Split(targetShards, ",")), workflowName, 10*time.Second)
	// SwitchTraffic
	output, err = reshard("SwitchTraffic")
	require.NoError(t, err, output)
	output, err = reshard("Complete")
	require.NoError(t, err, output)

	// When Reshard completes, it has already deleted the source shards from the topo server.
	// We just need to shutdown the vttablets, and remove them from the cluster.
	removeShardsFromKeyspace(t, clusterInstance, keyspaceName, sourceShards)
	return nil
}

// removeShardsFromKeyspace terminates the tablets of the given shards and drops the shards from the cluster.
func removeShardsFromKeyspace(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName string, shards string) {
	ctx := t.Context()
	for shardName := range strings.SplitSeq(shards, ",") {
		err := clusterInstance.RemoveShard(ctx, keyspaceName, shardName)
		require.NoError(t, err)
	}
}

// AddShardsToKeyspace starts the given shards on a running keyspace, each with a primary and two replicas,
// and waits for all their tablets to serve.
func AddShardsToKeyspace(t *testing.T, clusterInstance *vitesst.Cluster, keyspaceName string, shardNames []string) {
	t.Helper()
	ctx := t.Context()
	for _, shardName := range shardNames {
		shard, err := clusterInstance.AddShard(t, ctx, keyspaceName, shardName, 2, 0)
		require.NoError(t, err)
		for _, tablet := range shard.Tablets() {
			err = tablet.WaitForTabletStatus(ctx, 2*time.Minute, "SERVING")
			require.NoError(t, err)
		}
	}
}

// keyspaceShards returns the runtime handles of the named shards of a keyspace.
func keyspaceShards(clusterInstance *vitesst.Cluster, keyspaceName string, shardNames []string) []*vitesst.Shard {
	shards := make([]*vitesst.Shard, 0, len(shardNames))
	for _, shardName := range shardNames {
		if shard := clusterInstance.Keyspace(keyspaceName).Shard(shardName); shard != nil {
			shards = append(shards, shard)
		}
	}
	return shards
}

// WaitForVreplCatchup waits for a vreplication workflow to catch up on the primary of every given shard.
func WaitForVreplCatchup(t *testing.T, shards []*vitesst.Shard, workflow string, timeToWait time.Duration) {
	for _, shard := range shards {
		primary := primaryTablet(t, shard)
		waitForVReplicationToCatchup(t, primary, workflow, "vt_"+primary.Keyspace, timeToWait)
	}
}

// primaryTablet returns the shard's current primary tablet as recorded in the topology server.
func primaryTablet(t *testing.T, shard *vitesst.Shard) *vitesst.Tablet {
	ctx := t.Context()
	for _, tablet := range shard.Tablets() {
		record, err := tablet.TabletProto(ctx)
		if err != nil {
			continue
		}
		if record.Type == topodatapb.TabletType_PRIMARY {
			return tablet
		}
	}
	require.FailNowf(t, "no primary tablet", "shard %s has no primary tablet", shard.Ref())
	return nil
}

// waitForVReplicationToCatchup waits until the workflow has copied all of its rows on the given tablet.
func waitForVReplicationToCatchup(t *testing.T, tablet *vitesst.Tablet, workflow, database string, duration time.Duration) {
	queries := [3]string{
		fmt.Sprintf(`select count(*) from %s.vreplication where workflow = "%s" and db_name = "%s" and pos = ''`,
			sidecar.DefaultName, workflow, database),
		fmt.Sprintf("select count(*) from information_schema.tables where table_schema='%s' and table_name='copy_state' limit 1",
			sidecar.DefaultName),
		fmt.Sprintf(`select count(*) from %s.copy_state where vrepl_id in (select id from %s.vreplication where workflow = "%s" and db_name = "%s")`,
			sidecar.DefaultName, sidecar.DefaultName, workflow, database),
	}
	expected := [3]string{"[INT64(0)]", "[INT64(1)]", "[INT64(0)]"}

	ctx := t.Context()
	waitDuration := 500 * time.Millisecond
	for idx, query := range queries {
		for duration > 0 {
			res, err := tablet.QueryTabletWithDB(ctx, query, "")
			if err != nil {
				log.Error(fmt.Sprintf("Error in vreplication catchup query - %v", err))
			} else if len(res.Rows) > 0 && fmt.Sprintf("%v", res.Rows[0]) == expected[idx] {
				break
			}
			time.Sleep(waitDuration)
			duration -= waitDuration
		}
		require.Greaterf(t, duration, time.Duration(0),
			"WaitForVReplicationToCatchup timed out for workflow %s, keyspace %s", workflow, database)
	}
}

type Warn struct {
	Level string
	Code  uint16
	Msg   string
}

func ToWarn(row sqltypes.Row) Warn {
	code, _ := row[1].ToUint16()
	return Warn{
		Level: row[0].ToString(),
		Code:  code,
		Msg:   row[2].ToString(),
	}
}
