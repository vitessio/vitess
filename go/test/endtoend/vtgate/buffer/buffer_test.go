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

/*
Test the vtgate master buffer.

During a master failover, vtgate should automatically buffer (stall) requests
for a configured time and retry them after the failover is over.

The test reproduces such a scenario as follows:
- run two threads, the first thread continuously executes a critical read and the second executes a write (UPDATE)
- vtctl PlannedReparentShard runs a master failover
- both threads should not see any error during the failover
*/

package buffer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	tabletpb "vitess.io/vitess/go/vt/proto/topodata"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	vtParams              mysql.ConnParams
	keyspaceUnshardedName = "ks1"
	cell                  = "zone1"
	hostname              = "localhost"
	sqlSchema             = `
	create table buffer(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
	wg       = &sync.WaitGroup{}
	tmClient = tmc.NewClient()
)

const (
	criticalReadRowID          = 1
	updateRowID                = 2
	demoteMasterQuery          = "SET GLOBAL read_only = ON;FLUSH TABLES WITH READ LOCK;UNLOCK TABLES;"
	disableSemiSyncMasterQuery = "SET GLOBAL rpl_semi_sync_master_enabled = 0"
	enableSemiSyncMasterQuery  = "SET GLOBAL rpl_semi_sync_master_enabled = 1"
	masterPositionQuery        = "SELECT @@GLOBAL.gtid_executed;"
	promoteSlaveQuery          = "STOP SLAVE;RESET SLAVE ALL;SET GLOBAL read_only = OFF;"
)

//threadParams is set of params passed into read and write threads
type threadParams struct {
	writable                   bool
	quit                       bool
	rpcs                       int        // Number of queries successfully executed.
	errors                     int        // Number of failed queries.
	waitForNotification        chan bool  // Channel used to notify the main thread that this thread executed
	notifyLock                 sync.Mutex // notifyLock guards the two fields notifyAfterNSuccessfulRpcs/rpcsSoFar.
	notifyAfterNSuccessfulRpcs int        // If 0, notifications are disabled
	rpcsSoFar                  int        // Number of RPCs at the time a notification was requested
	i                          int        //
	commitErrors               int
	executeFunction            func(c *threadParams, conn *mysql.Conn) error // Implement the method for read/update.
}

// Thread which constantly executes a query on vtgate.
func (c *threadParams) threadRun() {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		println(err.Error())
	}
	defer conn.Close()
	for !c.quit {
		err = c.executeFunction(c, conn)
		if err != nil {
			c.errors++
			println(err.Error())
		}
		c.rpcs++
		// If notifications are requested, check if we already executed the
		// required number of successful RPCs.
		// Use >= instead of == because we can miss the exact point due to
		// slow thread scheduling.
		c.notifyLock.Lock()
		if c.notifyAfterNSuccessfulRpcs != 0 && c.rpcs >= (c.notifyAfterNSuccessfulRpcs+c.rpcsSoFar) {
			c.waitForNotification <- true
			c.notifyAfterNSuccessfulRpcs = 0
		}
		c.notifyLock.Unlock()
		// Wait 10ms seconds between two attempts.
		time.Sleep(10 * time.Millisecond)
	}
	wg.Done()
}

func (c *threadParams) setNotifyAfterNSuccessfulRpcs(n int) {
	c.notifyLock.Lock()
	c.notifyAfterNSuccessfulRpcs = n
	c.rpcsSoFar = c.rpcs
	c.notifyLock.Unlock()
}

func (c *threadParams) stop() {
	c.quit = true
}

func readExecute(c *threadParams, conn *mysql.Conn) error {
	_, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM buffer WHERE id = %d", criticalReadRowID), 1000, true)
	return err
}

func updateExecute(c *threadParams, conn *mysql.Conn) error {
	attempts := c.i
	// Value used in next UPDATE query. Increased after every query.
	c.i++
	conn.ExecuteFetch("begin", 1000, true)

	_, err := conn.ExecuteFetch(fmt.Sprintf("UPDATE buffer SET msg='update %d' WHERE id = %d", attempts, updateRowID), 1000, true)

	// Sleep between [0, 1] seconds to prolong the time the transaction is in
	// flight. This is more realistic because applications are going to keep
	// their transactions open for longer as well.
	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)

	if err == nil {
		fmt.Printf("update %d affected", attempts)
		_, err = conn.ExecuteFetch("commit", 1000, true)
		if err != nil {
			_, errRollback := conn.ExecuteFetch("rollback", 1000, true)
			if errRollback != nil {
				fmt.Print("Error in rollback", errRollback.Error())
			}
			c.commitErrors++
			if c.commitErrors > 1 {
				return err
			}
			fmt.Printf("UPDATE %d failed during ROLLBACK. This is okay once because we do not support buffering it. err: %s", attempts, err.Error())
		}
	}
	if err != nil {
		_, errRollback := conn.ExecuteFetch("rollback", 1000, true)
		if errRollback != nil {
			fmt.Print("Error in rollback", errRollback.Error())
		}
		c.commitErrors++
		if c.commitErrors > 1 {
			return err
		}
		fmt.Printf("UPDATE %d failed during COMMIT with err: %s.This is okay once because we do not support buffering it.", attempts, err.Error())
	}
	return nil
}

func createCluster() (*cluster.LocalProcessCluster, int) {
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return nil, 1
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceUnshardedName,
		SchemaSQL: sqlSchema,
	}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
		return nil, 1
	}

	clusterInstance.VtGateExtraArgs = []string{
		"-enable_buffer",
		// Long timeout in case failover is slow.
		"-buffer_window", "10m",
		"-buffer_max_failover_duration", "10m",
		"-buffer_min_time_between_failovers", "20m"}

	// Start vtgate
	if err := clusterInstance.StartVtgate(); err != nil {
		return nil, 1
	}
	vtParams = mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	rand.Seed(time.Now().UnixNano())
	return clusterInstance, 0
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func TestBufferInternalReparenting(t *testing.T) {
	testBufferBase(t, false)
}

func TestBufferExternalReparenting(t *testing.T) {
	testBufferBase(t, true)
}

func testBufferBase(t *testing.T, isExternalParent bool) {
	clusterInstance, exitCode := createCluster()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Insert two rows for the later threads (critical read, update).
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "'critical read'"))
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "'update'"))

	//Start both threads.
	readThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: readExecute, waitForNotification: make(chan bool)}
	wg.Add(1)
	go readThreadInstance.threadRun()
	updateThreadInstance := &threadParams{writable: false, quit: false, rpcs: 0, errors: 0, notifyAfterNSuccessfulRpcs: 0, rpcsSoFar: 0, executeFunction: updateExecute, i: 1, commitErrors: 0, waitForNotification: make(chan bool)}
	wg.Add(1)
	go updateThreadInstance.threadRun()

	// Verify they got at least 2 RPCs through.
	readThreadInstance.setNotifyAfterNSuccessfulRpcs(2)
	updateThreadInstance.setNotifyAfterNSuccessfulRpcs(2)

	<-readThreadInstance.waitForNotification
	<-updateThreadInstance.waitForNotification

	// Execute the failover.
	readThreadInstance.setNotifyAfterNSuccessfulRpcs(10)
	updateThreadInstance.setNotifyAfterNSuccessfulRpcs(10)

	if isExternalParent {
		externalReparenting(ctx, t, clusterInstance)
	} else {
		//reparent call
		clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
			fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
			"-new_master", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)
	}

	<-readThreadInstance.waitForNotification
	<-updateThreadInstance.waitForNotification

	// Stop threads
	readThreadInstance.stop()
	updateThreadInstance.stop()

	// Both threads must not see any error
	assert.Equal(t, 0, readThreadInstance.errors)
	assert.Equal(t, 0, updateThreadInstance.errors)

	//At least one thread should have been buffered.
	//This may fail if a failover is too fast. Add retries then.
	resp, err := http.Get(clusterInstance.VtgateProcess.VerifyURL)
	require.NoError(t, err)
	label := fmt.Sprintf("%s.%s", keyspaceUnshardedName, "0")
	inFlightMax := 0
	masterPromotedCount := 0
	durationMs := 0
	bufferingStops := 0
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			panic(err)
		}
		inFlightMax = getVarFromVtgate(t, label, "BufferLastRequestsInFlightMax", resultMap)
		masterPromotedCount = getVarFromVtgate(t, label, "HealthcheckMasterPromoted", resultMap)
		durationMs = getVarFromVtgate(t, label, "BufferFailoverDurationSumMs", resultMap)
		bufferingStops = getVarFromVtgate(t, "NewMasterSeen", "BufferStops", resultMap)
	}
	if inFlightMax == 0 {
		// Missed buffering is okay when we observed the failover during the
		// COMMIT (which cannot trigger the buffering).
		assert.Greater(t, updateThreadInstance.commitErrors, 0, "No buffering took place and the update thread saw no error during COMMIT. But one of it must happen.")
	} else {
		assert.Greater(t, inFlightMax, 0)
	}

	// There was a failover and the HealthCheck module must have seen it.
	if masterPromotedCount > 0 {
		assert.Greater(t, masterPromotedCount, 0)
	}

	if durationMs > 0 {
		// Number of buffering stops must be equal to the number of seen failovers.
		assert.Equal(t, masterPromotedCount, bufferingStops)
	}
	wg.Wait()
	clusterInstance.Teardown()
}

func getVarFromVtgate(t *testing.T, label string, param string, resultMap map[string]interface{}) int {
	paramVal := 0
	var err error
	object := reflect.ValueOf(resultMap[param])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			if strings.Contains(key.String(), label) {
				v := object.MapIndex(key)
				s := fmt.Sprintf("%v", v.Interface())
				paramVal, err = strconv.Atoi(s)
				require.NoError(t, err)
			}
		}
	}
	return paramVal
}

func externalReparenting(ctx context.Context, t *testing.T, clusterInstance *cluster.LocalProcessCluster) {
	start := time.Now()

	// Demote master Query
	master := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
	replica := clusterInstance.Keyspaces[0].Shards[0].Vttablets[1]
	oldMaster := master
	newMaster := replica
	master.VttabletProcess.QueryTablet(demoteMasterQuery, keyspaceUnshardedName, true)
	if master.VttabletProcess.EnableSemiSync {
		master.VttabletProcess.QueryTablet(disableSemiSyncMasterQuery, keyspaceUnshardedName, true)
	}

	// Wait for replica to catch up to master.
	waitForReplicationPos(ctx, t, master, replica, 60.0)

	duration := time.Since(start)
	minUnavailabilityInS := 1.0
	if duration.Seconds() < minUnavailabilityInS {
		w := minUnavailabilityInS - duration.Seconds()
		fmt.Printf("Waiting for %.1f seconds because the failover was too fast (took only %.3f seconds)", w, duration.Seconds())
		time.Sleep(time.Duration(w) * time.Second)
	}

	// Promote replica to new master.
	replica.VttabletProcess.QueryTablet(promoteSlaveQuery, keyspaceUnshardedName, true)

	if replica.VttabletProcess.EnableSemiSync {
		replica.VttabletProcess.QueryTablet(enableSemiSyncMasterQuery, keyspaceUnshardedName, true)
	}

	// Configure old master to replicate from new master.
	_, gtID := getMasterPosition(ctx, t, newMaster)

	// Use 'localhost' as hostname because Travis CI worker hostnames
	// are too long for MySQL replication.
	changeMasterCommands := fmt.Sprintf("RESET SLAVE;SET GLOBAL gtid_slave_pos = '%s';CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d ,MASTER_USER='vt_repl', MASTER_USE_GTID = slave_pos;START SLAVE;", gtID, "localhost", newMaster.MySQLPort)
	oldMaster.VttabletProcess.QueryTablet(changeMasterCommands, keyspaceUnshardedName, true)

	// Notify the new vttablet master about the reparent.
	clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", newMaster.Alias)
}

func waitForReplicationPos(ctx context.Context, t *testing.T, tabletA *cluster.Vttablet, tabletB *cluster.Vttablet, timeout float64) {
	replicationPosA, _ := getMasterPosition(ctx, t, tabletA)
	for {
		replicationPosB, _ := getMasterPosition(ctx, t, tabletB)
		if positionAtLeast(t, tabletA, replicationPosB, replicationPosA) {
			break
		}
		msg := fmt.Sprintf("%s's replication position to catch up to %s's;currently at: %s, waiting to catch up to: %s", tabletB.Alias, tabletA.Alias, replicationPosB, replicationPosA)
		waitStep(t, msg, timeout, 0.01)
	}
}

func getMasterPosition(ctx context.Context, t *testing.T, tablet *cluster.Vttablet) (string, string) {
	vtablet := getTablet(tablet.GrpcPort)
	newPos, err := tmClient.MasterPosition(ctx, vtablet)
	require.NoError(t, err)
	gtID := strings.SplitAfter(newPos, "/")[1]
	return newPos, gtID
}

func positionAtLeast(t *testing.T, tablet *cluster.Vttablet, a string, b string) bool {
	isAtleast := false
	val, err := tablet.MysqlctlProcess.ExecuteCommandWithOutput("position", "at_least", a, b)
	require.NoError(t, err)
	if strings.Contains(val, "true") {
		isAtleast = true
	}
	return isAtleast
}

func waitStep(t *testing.T, msg string, timeout float64, sleepTime float64) float64 {
	timeout = timeout - sleepTime
	if timeout < 0.0 {
		t.Errorf("timeout waiting for condition '%s'", msg)
	}
	time.Sleep(time.Duration(sleepTime) * time.Second)
	return timeout
}

func getTablet(tabletGrpcPort int) *tabletpb.Tablet {
	portMap := make(map[string]int32)
	portMap["grpc"] = int32(tabletGrpcPort)
	return &tabletpb.Tablet{Hostname: hostname, PortMap: portMap}
}
