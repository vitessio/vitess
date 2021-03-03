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

// Tests the robustness and resiliency of vtworkers.

package worker

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/sharding"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	master   *cluster.Vttablet
	replica1 *cluster.Vttablet
	rdOnly1  *cluster.Vttablet

	shard0Master  *cluster.Vttablet
	shard0Replica *cluster.Vttablet
	shard0RdOnly1 *cluster.Vttablet

	shard1Master  *cluster.Vttablet
	shard1Replica *cluster.Vttablet
	shard1RdOnly1 *cluster.Vttablet

	localCluster     *cluster.LocalProcessCluster
	cell             = cluster.DefaultCell
	hostname         = "localhost"
	keyspaceName     = "test_keyspace"
	shardName        = "0"
	shardTablets     []*cluster.Vttablet
	shard0Tablets    []*cluster.Vttablet
	shard1Tablets    []*cluster.Vttablet
	workerTestOffset = 0
	commonTabletArg  = []string{
		"-binlog_use_v3_resharding_mode=true"}
	vtWorkerTest = `
	create table worker_test (
	id bigint unsigned,
	sid int unsigned,
	msg varchar(64),
	primary key (id),
    index by_msg (msg)
	) Engine=InnoDB`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
        "worker_test": {
        "column_vindexes": [
          {
            "column": "sid",
            "name": "hash"
          }
        ]
      }
    }
}`
)

func TestReparentDuringWorkerCopy(t *testing.T) {
	defer cluster.PanicHandler(t)
	_, err := initializeCluster(t, false)
	defer localCluster.Teardown()
	require.Nil(t, err)
	initialSetup(t)
	verifySuccessfulWorkerCopyWithReparent(t, false)
}

func TestReparentDuringWorkerCopyMysqlDown(t *testing.T) {
	defer cluster.PanicHandler(t)
	_, err := initializeCluster(t, false)
	defer localCluster.Teardown()
	require.Nil(t, err)
	initialSetup(t)
	verifySuccessfulWorkerCopyWithReparent(t, true)
}

func TestWebInterface(t *testing.T) {
	defer cluster.PanicHandler(t)
	_, err := initializeCluster(t, true)
	require.Nil(t, err)
	defer localCluster.Teardown()
	err = localCluster.StartVtworker(cell, "--use_v3_resharding_mode=true")
	assert.Nil(t, err)
	baseURL := fmt.Sprintf("http://localhost:%d", localCluster.VtworkerProcess.Port)

	// Wait for /status to become available.
	startTime := time.Now()
	for {
		resp, err := http.Get(baseURL + "/status")
		if err != nil && !time.Now().After(startTime.Add(10*time.Second)) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if resp.StatusCode == 200 || time.Now().After(startTime.Add(10*time.Second)) {
			break
		}
	}

	// Run the command twice to make sure it's idempotent.
	i := 0
	for i < 2 {
		data := url.Values{"message": {"pong"}}
		http.DefaultClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
		resp, err := http.Post(baseURL+"/Debugging/Ping", "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
		assert.Nil(t, err)
		assert.Equal(t, 307, resp.StatusCode)

		// Wait for the Ping command to finish.
		pollForVars(t, "done")
		// Verify that the command logged something and it's available at /status.
		resp, err = http.Get(baseURL + "/status")
		assert.Nil(t, err)
		if resp.StatusCode == 200 {
			respByte, _ := ioutil.ReadAll(resp.Body)
			respStr := string(respByte)
			assert.Contains(t, respStr, "Ping command was called with message: 'pong'", fmt.Sprintf("Command did not log output to /status: %s", respStr))
		}

		// Reset the job.
		_, err = http.Get(baseURL + "/reset")
		assert.Nil(t, err)
		resp, err = http.Get(baseURL + "/status")
		assert.Nil(t, err)
		if resp.StatusCode == 200 {
			respByte, _ := ioutil.ReadAll(resp.Body)
			statusAfterReset := string(respByte)
			assert.Contains(t, statusAfterReset, "This worker is idle.", "/status does not indicate that the reset was successful")
		}
		i++
	}

	err = localCluster.VtworkerProcess.TearDown()
	assert.Nil(t, err)

}

func initialSetup(t *testing.T) {

	runShardTablets(t, "0", shardTablets, true)

	// create the split shards
	runShardTablets(t, "-80", shard0Tablets, false)
	runShardTablets(t, "80-", shard1Tablets, false)

	// insert values
	insertValues(master, "shard-0", 1, 4000, 0)
	insertValues(master, "shard-1", 4, 4000, 1)

	// wait for replication position
	cluster.WaitForReplicationPos(t, master, rdOnly1, "localhost", 60)

	copySchemaToDestinationShard(t)
}

func verifySuccessfulWorkerCopyWithReparent(t *testing.T, isMysqlDown bool) {

	// Verifies that vtworker can successfully copy data for a SplitClone.

	// Order of operations:
	// 1. Run a background vtworker
	// 2. Wait until the worker successfully resolves the destination masters.
	// 3. Reparent the destination tablets
	// 4. Wait until the vtworker copy is finished
	// 5. Verify that the worker was forced to reresolve topology and retry writes
	//	  due to the reparent.
	// 6. Verify that the data was copied successfully to both new shards

	err := localCluster.StartVtworker(cell, "--use_v3_resharding_mode=true")
	assert.Nil(t, err)

	// --max_tps is only specified to enable the throttler and ensure that the
	// code is executed. But the intent here is not to throttle the test, hence
	// the rate limit is set very high.

	var args []string

	args = append(args, "SplitClone", "--offline=false",
		"--destination_writer_count", "1",
		"--min_healthy_rdonly_tablets", "1",
		"--max_tps", "9999")

	// --chunk_count is 2 because rows are currently ordered by primary key such
	// that all rows of the first shard come first and then the second shard.
	// Make the clone as slow as necessary such that there is enough time to
	// run PlannedReparent in the meantime.

	args = append(args, "--source_reader_count", "2",
		"--chunk_count", "2",
		"--min_rows_per_chunk", "1",
		"--write_query_max_rows", "1",
		"test_keyspace/0")

	proc, err := localCluster.VtworkerProcess.ExecuteCommandInBg(args...)
	assert.Nil(t, err)

	if isMysqlDown {
		// vtworker is blocked at this point. This is a good time to test that its
		// throttler server is reacting to RPCs.
		sharding.CheckThrottlerService(t, fmt.Sprintf("%s:%d", hostname, localCluster.VtworkerProcess.GrpcPort),
			[]string{"test_keyspace/-80", "test_keyspace/80-"}, 9999, *localCluster)

		pollForVars(t, "cloning the data (online)")

		// Stop MySql
		var mysqlCtlProcessList []*exec.Cmd

		for _, tablet := range []*cluster.Vttablet{shard0Master, shard1Master} {
			tablet.MysqlctlProcess.InitMysql = false
			sqlProc, err := tablet.MysqlctlProcess.StopProcess()
			assert.Nil(t, err)
			mysqlCtlProcessList = append(mysqlCtlProcessList, sqlProc)
		}

		// Wait for mysql processes to stop
		for _, sqlProc := range mysqlCtlProcessList {
			if err := sqlProc.Wait(); err != nil {
				t.Fatal(err)
			}
		}

		// If MySQL is down, we wait until vtworker retried at least once to make
		// sure it reached the point where a write failed due to MySQL being down.
		// There should be two retries at least, one for each destination shard.
		pollForVarsWorkerRetryCount(t, 1)

		// Bring back masters. Since we test with semi-sync now, we need at least
		// one replica for the new master. This test is already quite expensive,
		// so we bring back the old master and then let it be converted to a
		// replica by PRS, rather than leaving the old master down and having a
		// third replica up the whole time.

		// start mysql
		var mysqlCtlProcessStartList []*exec.Cmd

		for _, tablet := range []*cluster.Vttablet{shard0Master, shard1Master} {
			tablet.MysqlctlProcess.InitMysql = false
			sqlProc, err := tablet.MysqlctlProcess.StartProcess()
			assert.Nil(t, err)
			mysqlCtlProcessStartList = append(mysqlCtlProcessStartList, sqlProc)
		}

		// Wait for mysql processes to start
		for _, sqlProc := range mysqlCtlProcessStartList {
			if err := sqlProc.Wait(); err != nil {
				t.Fatal(err)
			}
		}
	} else {

		// NOTE: There is a race condition around this:
		// It's possible that the SplitClone vtworker command finishes before the
		// PlannedReparentShard vtctl command, which we start below, succeeds.
		// Then the test would fail because vtworker did not have to retry.
		//
		// To workaround this, the test takes a parameter to increase the number of
		// rows that the worker has to copy (with the idea being to slow the worker
		// down).
		// You should choose a value for num_insert_rows, such that this test
		// passes for your environment (trial-and-error...)
		// Make sure that vtworker got past the point where it picked a master
		// for each destination shard ("finding targets" state).
		pollForVars(t, "cloning the data (online)")

	}

	// Reparent away from the old masters.
	localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
		"test_keyspace/-80", "-new_master", shard0Replica.Alias)

	localCluster.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
		"test_keyspace/80-", "-new_master", shard1Replica.Alias)

	proc.Wait()

	// Verify that we were forced to re-resolve and retry.
	pollForVarsWorkerRetryCount(t, 1)

	err = localCluster.VtworkerProcess.TearDown()
	assert.Nil(t, err)

	cluster.WaitForReplicationPos(t, shard0Replica, shard0RdOnly1, "localhost", 60)
	cluster.WaitForReplicationPos(t, shard1Replica, shard1RdOnly1, "localhost", 60)

	err = localCluster.VtworkerProcess.ExecuteVtworkerCommand(localCluster.GetAndReservePort(), localCluster.GetAndReservePort(), "-cell", cell,
		"--use_v3_resharding_mode=true",
		"SplitClone",
		"--online=false",
		"--min_healthy_rdonly_tablets", "1",
		"test_keyspace/0")
	assert.Nil(t, err)

	// Make sure that everything is caught up to the same replication point
	runSplitDiff(t, "test_keyspace/-80")
	runSplitDiff(t, "test_keyspace/80-")
	assertShardDataEqual(t, "0", master, shard0Replica)
	assertShardDataEqual(t, "1", master, shard1Replica)
}

func assertShardDataEqual(t *testing.T, shardNum string, sourceTablet *cluster.Vttablet, destinationTablet *cluster.Vttablet) {
	messageStr := fmt.Sprintf("shard-%s", shardNum)
	selectQuery := "select id,sid,msg from worker_test where msg = '" + messageStr + "' order by id asc"
	qrSource, err := sourceTablet.VttabletProcess.QueryTablet(selectQuery, keyspaceName, true)
	assert.Nil(t, err)

	// Make sure all the right rows made it from the source to the destination
	qrDestination, err := destinationTablet.VttabletProcess.QueryTablet(selectQuery, keyspaceName, true)
	assert.Nil(t, err)
	assert.Equal(t, len(qrSource.Rows), len(qrDestination.Rows))

	assert.Equal(t, fmt.Sprint(qrSource.Rows), fmt.Sprint(qrDestination.Rows))

	// Make sure that there are no extra rows on the destination
	countQuery := "select count(*) from worker_test"
	qrDestinationCount, err := destinationTablet.VttabletProcess.QueryTablet(countQuery, keyspaceName, true)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("%d", len(qrDestination.Rows)), fmt.Sprintf("%s", qrDestinationCount.Rows[0][0].ToBytes()))

}

// Runs a vtworker SplitDiff on the given keyspace/shard.
func runSplitDiff(t *testing.T, keyspaceShard string) {

	err := localCluster.VtworkerProcess.ExecuteVtworkerCommand(localCluster.GetAndReservePort(),
		localCluster.GetAndReservePort(),
		"-cell", cell,
		"--use_v3_resharding_mode=true",
		"SplitDiff",
		"--min_healthy_rdonly_tablets", "1",
		keyspaceShard)
	assert.Nil(t, err)

}

func pollForVars(t *testing.T, mssg string) {
	startTime := time.Now()
	var resultMap map[string]interface{}
	var err error
	var workerState string
	for {
		resultMap, err = localCluster.VtworkerProcess.GetVars()
		assert.Nil(t, err)
		workerState = fmt.Sprintf("%v", reflect.ValueOf(resultMap["WorkerState"]))
		if strings.Contains(workerState, mssg) || (time.Now().After(startTime.Add(60 * time.Second))) {
			break
		}
		continue
	}
	assert.Contains(t, workerState, mssg)
}

func pollForVarsWorkerRetryCount(t *testing.T, count int) {
	startTime := time.Now()
	var resultMap map[string]interface{}
	var err error
	var workerRetryCountInt int
	for {
		resultMap, err = localCluster.VtworkerProcess.GetVars()
		if err != nil {
			continue
		}
		workerRetryCount := fmt.Sprintf("%v", reflect.ValueOf(resultMap["WorkerRetryCount"]))
		workerRetryCountInt, err = strconv.Atoi(workerRetryCount)
		assert.Nil(t, err)
		if workerRetryCountInt > count || (time.Now().After(startTime.Add(60 * time.Second))) {
			break
		}
		continue
	}
	assert.Greater(t, workerRetryCountInt, count)
}

// vttablet: the Tablet instance to modify.
// msg: the value of `msg` column.
// numValues: number of rows to be inserted.
func insertValues(tablet *cluster.Vttablet, msg string, sid int, numValues int, initialVal int) {

	// For maximum performance, multiple values are inserted in one statement.
	// However, when the statements are too long, queries will timeout and
	// vttablet will kill them. Therefore, we chunk it into multiple statements.
	maxChunkSize := 100 * 1000

	var fullList []int
	for i := 1; i <= numValues; i++ {
		fullList = append(fullList, i)
	}
	m := getChunkArr(fullList, maxChunkSize)

	for i := 0; i < len(m); i++ {
		valueStr := ""
		for j := 0; j < len(m[i]); j++ {
			if m[i][j] != m[i][0] {
				valueStr += ","
			}
			rowID := j*2 + initialVal
			valueStr = valueStr + fmt.Sprintf("(%d, %d, '%s')", rowID, sid, msg)
		}
		workerTestOffset += len(m[i])
		tablet.VttabletProcess.QueryTablet(fmt.Sprintf("insert into worker_test(id, sid, msg) values %s", valueStr), keyspaceName, true)
	}
}

func getChunkArr(fullList []int, chunkSize int) [][]int {
	var m [][]int
	for i := 0; i < len(fullList); i += chunkSize {
		end := i + chunkSize
		if end > len(fullList) {
			end = len(fullList)
		}
		m = append(m, fullList[i:end])
	}
	return m
}

// shardName: the name of the shard to start tablets in
// tabletArr: an instance of ShardTablets for the given shard
// createTable: boolean, True iff we should create a table on the tablets
func runShardTablets(t *testing.T, shardName string, tabletArr []*cluster.Vttablet, createTable bool) error {
	//Handles all the necessary work for initially running a shard's tablets.

	//	This encompasses the following steps:
	// 1. (optional) Create db
	// 2. Starting vttablets and let themselves init them
	// 3. Waiting for the appropriate vttablet state
	// 4. Force reparent to the master tablet
	// 5. RebuildKeyspaceGraph
	// 7. (optional) Running initial schema setup

	// Start tablets.
	for _, tablet := range tabletArr {
		if err := tablet.VttabletProcess.CreateDB(keyspaceName); err != nil {
			return err
		}
		err := tablet.VttabletProcess.Setup()
		require.Nil(t, err)
	}

	// Reparent to choose an initial master and enable replication.
	err := localCluster.VtctlclientProcess.InitShardMaster(keyspaceName, shardName, cell, tabletArr[0].TabletUID)
	require.Nil(t, err)

	for {
		result, err := localCluster.VtctlclientProcess.ExecuteCommandWithOutput("GetShard", fmt.Sprintf("test_keyspace/%s", shardName))
		assert.Nil(t, err)

		var shardInfo topodatapb.Shard
		err = json2.Unmarshal([]byte(result), &shardInfo)
		assert.Nil(t, err)

		if int(shardInfo.MasterAlias.Uid) == tabletArr[0].TabletUID {
			break
		}
		time.Sleep(10 * time.Second)
		continue
	}

	err = localCluster.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_keyspace")
	assert.Nil(t, err)

	// Enforce a health check instead of waiting for the next periodic one.
	// (saves up to 1 second execution time on average)
	for _, tablet := range []*cluster.Vttablet{tabletArr[1], tabletArr[2]} {
		err = localCluster.VtctlclientProcess.ExecuteCommand("RunHealthCheck", tablet.Alias)
		assert.Nil(t, err)
	}

	// Wait for tablet state to change after starting all tablets. This allows
	// us to start all tablets at once, instead of sequentially waiting.
	// NOTE: Replication has to be enabled first or the health check will
	// set a replica or rdonly tablet back to NOT_SERVING.

	for _, tablet := range tabletArr {
		err = tablet.VttabletProcess.WaitForTabletType("SERVING")
		require.Nil(t, err)
	}

	if createTable {
		err = localCluster.VtctlclientProcess.ApplySchema(keyspaceName, vtWorkerTest)
		assert.Nil(t, err)

		err = localCluster.VtctlclientProcess.ApplyVSchema(keyspaceName, vSchema)
		assert.Nil(t, err)
	}

	return err
}

func copySchemaToDestinationShard(t *testing.T) {
	for _, keyspaceShard := range []string{"test_keyspace/-80", "test_keyspace/80-"} {
		err := localCluster.VtctlclientProcess.ExecuteCommand("CopySchemaShard", "--exclude_tables", "unrelated", "test_keyspace/0", keyspaceShard)
		assert.Nil(t, err)
	}
}

func initializeCluster(t *testing.T, onlyTopo bool) (int, error) {

	localCluster = cluster.NewCluster(cell, hostname)

	// Start topo server
	err := localCluster.StartTopo()
	if err != nil {
		return 1, err
	}

	if onlyTopo {
		return 0, nil
	}
	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name: keyspaceName,
	}
	localCluster.Keyspaces = append(localCluster.Keyspaces, *keyspace)

	shard := &cluster.Shard{
		Name: shardName,
	}
	shard0 := &cluster.Shard{
		Name: "-80",
	}
	shard1 := &cluster.Shard{
		Name: "80-",
	}

	// Defining all the tablets
	master = localCluster.NewVttabletInstance("replica", 0, "")
	replica1 = localCluster.NewVttabletInstance("replica", 0, "")
	rdOnly1 = localCluster.NewVttabletInstance("rdonly", 0, "")
	shard0Master = localCluster.NewVttabletInstance("replica", 0, "")
	shard0Replica = localCluster.NewVttabletInstance("replica", 0, "")
	shard0RdOnly1 = localCluster.NewVttabletInstance("rdonly", 0, "")
	shard1Master = localCluster.NewVttabletInstance("replica", 0, "")
	shard1Replica = localCluster.NewVttabletInstance("replica", 0, "")
	shard1RdOnly1 = localCluster.NewVttabletInstance("rdonly", 0, "")

	shard.Vttablets = []*cluster.Vttablet{master, replica1, rdOnly1}
	shard0.Vttablets = []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly1}
	shard1.Vttablets = []*cluster.Vttablet{shard1Master, shard1Replica, shard1RdOnly1}

	localCluster.VtTabletExtraArgs = append(localCluster.VtTabletExtraArgs, commonTabletArg...)

	err = localCluster.SetupCluster(keyspace, []cluster.Shard{*shard, *shard0, *shard1})
	assert.Nil(t, err)

	// Start MySql
	var mysqlCtlProcessList []*exec.Cmd
	for _, shard := range localCluster.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			if proc, err := tablet.MysqlctlProcess.StartProcess(); err != nil {
				t.Fatal(err)
			} else {
				mysqlCtlProcessList = append(mysqlCtlProcessList, proc)
			}
		}
	}

	// Wait for mysql processes to start
	for _, proc := range mysqlCtlProcessList {
		if err := proc.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	shardTablets = []*cluster.Vttablet{master, replica1, rdOnly1}
	shard0Tablets = []*cluster.Vttablet{shard0Master, shard0Replica, shard0RdOnly1}
	shard1Tablets = []*cluster.Vttablet{shard1Master, shard1Replica, shard1RdOnly1}

	return 0, nil
}
