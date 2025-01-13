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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
)

const (
	DebugDelayCommitShard = "VT_DELAY_COMMIT_SHARD"
	DebugDelayCommitTime  = "VT_DELAY_COMMIT_TIME"
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
			log.Errorf("Error in connection - %v\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT count(*) FROM %v", tableName), 1, false)
		if err != nil {
			log.Errorf("Error in selecting - %v\n", err)
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
			log.Errorf("Error in cleanup deletion - %v\n", err)
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
	err := os.WriteFile(path.Join(os.Getenv("VTDATAROOT"), fileName), []byte(content), 0644)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := utils.ExecAllowError(t, conn, "commit")
		if err != nil {
			log.Errorf("Error in commit - %v", err)
		}
	}()
}

// DeleteFile deletes the file specified.
func DeleteFile(fileName string) {
	_ = os.Remove(path.Join(os.Getenv("VTDATAROOT"), fileName))
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
			ctx := context.Background()
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

func WaitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, ks string, shards []cluster.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard.Name] = true
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

func RunReshard(t *testing.T, clusterInstance *cluster.LocalProcessCluster, workflowName, keyspaceName string, sourceShards, targetShards string) error {
	rw := cluster.NewReshard(t, clusterInstance, workflowName, keyspaceName, targetShards, sourceShards)
	// Initiate Reshard.
	output, err := rw.Create()
	require.NoError(t, err, output)
	// Wait for vreplication to catchup. Should be very fast since we don't have a lot of rows.
	rw.WaitForVreplCatchup(10 * time.Second)
	// SwitchTraffic
	output, err = rw.SwitchReadsAndWrites()
	require.NoError(t, err, output)
	output, err = rw.Complete()
	require.NoError(t, err, output)

	// When Reshard completes, it has already deleted the source shards from the topo server.
	// We just need to shutdown the vttablets, and remove them from the cluster.
	removeShards(t, clusterInstance, keyspaceName, sourceShards)
	return nil
}

func removeShards(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceName string, shards string) {
	sourceShardsList := strings.Split(shards, ",")
	var remainingShards []cluster.Shard
	for idx, keyspace := range clusterInstance.Keyspaces {
		if keyspace.Name != keyspaceName {
			continue
		}
		for _, shard := range keyspace.Shards {
			if slices.Contains(sourceShardsList, shard.Name) {
				for _, vttablet := range shard.Vttablets {
					err := vttablet.VttabletProcess.TearDown()
					require.NoError(t, err)
				}
				continue
			}
			remainingShards = append(remainingShards, shard)
		}
		clusterInstance.Keyspaces[idx].Shards = remainingShards
	}
}

func AddShards(t *testing.T, clusterInstance *cluster.LocalProcessCluster, keyspaceName string, shardNames []string) {
	for _, shardName := range shardNames {
		t.Helper()
		shard, err := clusterInstance.AddShard(keyspaceName, shardName, 3, false, nil)
		require.NoError(t, err)
		clusterInstance.Keyspaces[0].Shards = append(clusterInstance.Keyspaces[0].Shards, *shard)
		for _, vttablet := range shard.Vttablets {
			err = vttablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING"})
			require.NoError(t, err)
		}
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
