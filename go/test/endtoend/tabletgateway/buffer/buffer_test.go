/*
Copyright 2020 The Vitess Authors.

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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
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
	wg = &sync.WaitGroup{}
)

const (
	criticalReadRowID          = 1
	updateRowID                = 2
	demoteMasterQuery          = "SET GLOBAL read_only = ON;FLUSH TABLES WITH READ LOCK;UNLOCK TABLES;"
	disableSemiSyncMasterQuery = "SET GLOBAL rpl_semi_sync_master_enabled = 0"
	enableSemiSyncMasterQuery  = "SET GLOBAL rpl_semi_sync_master_enabled = 1"
	promoteQuery               = "STOP SLAVE;RESET SLAVE ALL;SET GLOBAL read_only = OFF;"
)

//threadParams is set of params passed into read and write threads
type threadParams struct {
	quit                       bool
	rpcs                       int        // Number of queries successfully executed.
	errors                     int        // Number of failed queries.
	waitForNotification        chan bool  // Channel used to notify the main thread that this thread executed
	notifyLock                 sync.Mutex // notifyLock guards the two fields notifyAfterNSuccessfulRpcs/rpcsSoFar.
	notifyAfterNSuccessfulRpcs int        // If 0, notifications are disabled
	rpcsSoFar                  int        // Number of RPCs at the time a notification was requested
	index                      int        //
	internalErrs               int
	executeFunction            func(c *threadParams, conn *mysql.Conn) error // Implement the method for read/update.
	typ                        string
	reservedConn               bool
}

// Thread which constantly executes a query on vtgate.
func (c *threadParams) threadRun() {
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		log.Errorf("error connecting to mysql with params %v: %v", vtParams, err)
	}
	defer conn.Close()
	if c.reservedConn {
		_, err = conn.ExecuteFetch("set default_week_format = 1", 1000, true)
		if err != nil {
			c.errors++
			log.Errorf("error setting default_week_format: %v", err)
		}
	}
	for !c.quit {
		err = c.executeFunction(c, conn)
		if err != nil {
			c.errors++
			log.Errorf("error executing function %s: %v", c.typ, err)
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
	attempt := c.index
	c.index++
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT * FROM buffer WHERE id = %d", criticalReadRowID), 1000, true)

	if err != nil {
		log.Errorf("select attempt #%d, failed with err: %v", attempt, err)
		// For a reserved connection, read query can fail as it does not go through the gateway and
		// goes to tablet directly and later is directed to use Gateway if the error is caused due to cluster failover operation.
		if c.reservedConn {
			c.internalErrs++
			if c.internalErrs > 1 {
				log.Errorf("More Read Errors: %d", c.internalErrs)
				return err
			}
			log.Error("This is okay once because we do not support buffering it.")
			return nil
		}
		return err
	}

	log.Infof("select attempt #%d, rows: %d", attempt, len(qr.Rows))
	return nil
}

func updateExecute(c *threadParams, conn *mysql.Conn) error {
	attempt := c.index
	// Value used in next UPDATE query. Increased after every query.
	c.index++
	conn.ExecuteFetch("begin", 1000, true)

	result, err := conn.ExecuteFetch(fmt.Sprintf("UPDATE buffer SET msg='update %d' WHERE id = %d", attempt, updateRowID), 1000, true)

	// Sleep between [0, 1] seconds to prolong the time the transaction is in
	// flight. This is more realistic because applications are going to keep
	// their transactions open for longer as well.
	time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)

	if err == nil {
		log.Infof("update attempt #%d affected %v rows", attempt, result.RowsAffected)
		_, err = conn.ExecuteFetch("commit", 1000, true)
		if err != nil {
			log.Errorf("UPDATE #%d failed during COMMIT, err: %v", attempt, err)
			_, errRollback := conn.ExecuteFetch("rollback", 1000, true)
			if errRollback != nil {
				log.Errorf("Error in rollback #%d: %v", attempt, errRollback)
			}
			c.internalErrs++
			if c.internalErrs > 1 {
				log.Errorf("More Commit Errors: %d", c.internalErrs)
				return err
			}
			log.Error("This is okay once because we do not support buffering it.")
		}
		return nil
	}
	log.Errorf("UPDATE #%d failed with err: %v", attempt, err)
	_, errRollback := conn.ExecuteFetch("rollback", 1000, true)
	if errRollback != nil {
		log.Errorf("Error in rollback #%d: %v", attempt, errRollback)
	}
	c.internalErrs++
	if c.internalErrs > 1 {
		log.Errorf("More Rollback Errors: %d", c.internalErrs)
		return err
	}
	log.Error("This is okay once because we do not support buffering it.")
	return nil
}

func createCluster() (*cluster.LocalProcessCluster, int) {
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	clusterInstance.VtctldExtraArgs = []string{"-remote_operation_timeout", "30s", "-topo_etcd_lease_ttl", "40"}
	if err := clusterInstance.StartTopo(); err != nil {
		return nil, 1
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceUnshardedName,
		SchemaSQL: sqlSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{"-health_check_interval", "1s",
		"-queryserver-config-transaction-timeout", "20",
	}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
		return nil, 1
	}

	clusterInstance.VtGateExtraArgs = []string{
		"-enable_buffer",
		// Long timeout in case failover is slow.
		"-buffer_window", "10m",
		"-buffer_max_failover_duration", "10m",
		"-buffer_min_time_between_failovers", "20m",
		"-gateway_implementation", "tabletgateway"}

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
	require.Nil(t, err)
	return qr
}

func TestBufferReparenting(t *testing.T) {
	t.Run("TER without reserved connection", func(t *testing.T) {
		testBufferBase(t, true, false)
	})
	t.Run("TER with reserved connection", func(t *testing.T) {
		testBufferBase(t, true, true)
	})
	t.Run("PRS without reserved connections", func(t *testing.T) {
		testBufferBase(t, false, false)
	})
	t.Run("PRS with reserved connections", func(t *testing.T) {
		testBufferBase(t, false, true)
	})
}

var ctx = context.Background()

func testBufferBase(t *testing.T, isExternalParent bool, useReservedConn bool) {
	defer cluster.PanicHandler(t)
	clusterInstance, exitCode := createCluster()
	if exitCode != 0 {
		t.Fatal("failed to start cluster")
	}
	defer clusterInstance.Teardown()

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	// Insert two rows for the later threads (critical read, update).
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "'critical read'"))
	exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "'update'"))

	//Start both threads.
	readThreadInstance := &threadParams{
		index:               1,
		typ:                 "read",
		executeFunction:     readExecute,
		waitForNotification: make(chan bool),
		reservedConn:        useReservedConn,
	}
	wg.Add(1)
	go readThreadInstance.threadRun()
	updateThreadInstance := &threadParams{
		index:               1,
		typ:                 "write",
		executeFunction:     updateExecute,
		waitForNotification: make(chan bool),
		reservedConn:        useReservedConn,
	}
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
		err := externalReparenting(t, clusterInstance)
		require.NoError(t, err)
	} else {
		//reparent call
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "-keyspace_shard",
			fmt.Sprintf("%s/%s", keyspaceUnshardedName, "0"),
			"-new_master", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)
		require.NoError(t, err)
	}

	timeout := time.After(40 * time.Second)
	select {
	case <-readThreadInstance.waitForNotification:
	case <-timeout:
		timeout = time.After(100 * time.Millisecond)
		log.Error("failed to get read thread notification")
	}
	select {
	case <-updateThreadInstance.waitForNotification:
	case <-timeout:
		log.Error("failed to get update thread notification")
	}

	// Stop threads
	readThreadInstance.stop()
	updateThreadInstance.stop()

	// Both threads must not see any error
	assert.Zero(t, readThreadInstance.errors, "found errors in read queries")
	assert.Zero(t, updateThreadInstance.errors, "found errors in tx queries")

	//At least one thread should have been buffered.
	//This may fail if a failover is too fast. Add retries then.
	resp, err := http.Get(clusterInstance.VtgateProcess.VerifyURL)
	require.Nil(t, err)
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
		assert.Greater(t, updateThreadInstance.internalErrs, 0, "No buffering took place and the update thread saw no error during COMMIT. But one of it must happen.")
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
				require.Nil(t, err)
			}
		}
	}
	return paramVal
}

func externalReparenting(t *testing.T, clusterInstance *cluster.LocalProcessCluster) error {
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
	cluster.WaitForReplicationPos(t, master, replica, "localhost", 60.0)

	duration := time.Since(start)
	minUnavailabilityInS := 1.0
	if duration.Seconds() < minUnavailabilityInS {
		w := minUnavailabilityInS - duration.Seconds()
		log.Infof("Waiting for %.1f seconds because the failover was too fast (took only %.3f seconds)", w, duration.Seconds())
		time.Sleep(time.Duration(w) * time.Second)
	}

	// Promote replica to new master.
	replica.VttabletProcess.QueryTablet(promoteQuery, keyspaceUnshardedName, true)

	if replica.VttabletProcess.EnableSemiSync {
		replica.VttabletProcess.QueryTablet(enableSemiSyncMasterQuery, keyspaceUnshardedName, true)
	}

	// Configure old master to replicate from new master.

	_, gtID := cluster.GetMasterPosition(t, *newMaster, hostname)

	// Use 'localhost' as hostname because Travis CI worker hostnames
	// are too long for MySQL replication.
	changeMasterCommands := fmt.Sprintf("RESET SLAVE;SET GLOBAL gtid_slave_pos = '%s';CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d ,MASTER_USER='vt_repl', MASTER_USE_GTID = slave_pos;START SLAVE;", gtID, "localhost", newMaster.MySQLPort)
	oldMaster.VttabletProcess.QueryTablet(changeMasterCommands, keyspaceUnshardedName, true)

	// Notify the new vttablet master about the reparent.
	return clusterInstance.VtctlclientProcess.ExecuteCommand("TabletExternallyReparented", newMaster.Alias)
}
