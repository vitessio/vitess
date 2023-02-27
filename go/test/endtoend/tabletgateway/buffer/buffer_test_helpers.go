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
Test the vtgate buffering.

During a failover, vtgate should automatically buffer (stall) requests
for a configured time and retry them after the failover is over.

The test reproduces such a scenario as follows:
- run two threads, the first thread continuously executes a critical read and the second executes a write (UPDATE)
- vtctl PlannedReparentShard runs a failover
- both threads should not see any error during the failover
*/

package buffer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	keyspaceUnshardedName = "ks1"
	cell                  = "zone1"
	hostname              = "localhost"
	sqlSchema             = `
	create table buffer(
		id BIGINT NOT NULL,
		msg VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) Engine=InnoDB;`
)

const (
	criticalReadRowID = 1
	updateRowID       = 2
)

// threadParams is set of params passed into read and write threads
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
	slowQueries                bool
}

// Thread which constantly executes a query on vtgate.
func (c *threadParams) threadRun(wg *sync.WaitGroup, vtParams *mysql.ConnParams) {
	defer wg.Done()

	conn, err := mysql.Connect(context.Background(), vtParams)
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
}

func (c *threadParams) ExpectQueries(n int) {
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

	sel := "*"
	if c.slowQueries {
		sel = "*, SLEEP(1)"
	}
	qr, err := conn.ExecuteFetch(fmt.Sprintf("SELECT %s FROM buffer WHERE id = %d", sel, criticalReadRowID), 1000, true)

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
	dur := time.Duration(rand.Int31n(1000)) * time.Millisecond
	if c.slowQueries {
		dur = dur + 1*time.Second
	}
	time.Sleep(dur)

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

func (bt *BufferingTest) createCluster() (*cluster.LocalProcessCluster, int) {
	clusterInstance := cluster.NewCluster(cell, hostname)

	// Start topo server
	clusterInstance.VtctldExtraArgs = []string{"--remote_operation_timeout", "30s", "--topo_etcd_lease_ttl", "40"}
	if err := clusterInstance.StartTopo(); err != nil {
		return nil, 1
	}

	// Start keyspace
	keyspace := &cluster.Keyspace{
		Name:      keyspaceUnshardedName,
		SchemaSQL: sqlSchema,
		VSchema:   bt.VSchema,
	}
	clusterInstance.VtTabletExtraArgs = []string{
		"--health_check_interval", "1s",
		"--queryserver-config-transaction-timeout", "20",
	}
	if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
		return nil, 1
	}

	clusterInstance.VtGateExtraArgs = []string{
		"--enable_buffer",
		// Long timeout in case failover is slow.
		"--buffer_window", "10m",
		"--buffer_max_failover_duration", "10m",
		"--buffer_min_time_between_failovers", "20m",
		"--buffer_implementation", "keyspace_events",
		"--tablet_refresh_interval", "1s",
	}
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, bt.VtGateExtraArgs...)

	// Start vtgate
	clusterInstance.VtGatePlannerVersion = 0
	if err := clusterInstance.StartVtgate(); err != nil {
		return nil, 1
	}
	rand.Seed(time.Now().UnixNano())
	return clusterInstance, 0
}

type QueryEngine interface {
	ExpectQueries(count int)
}

type BufferingTest struct {
	Assert   func(t *testing.T, shard string, stats *VTGateBufferingStats)
	Failover func(t *testing.T, cluster *cluster.LocalProcessCluster, keyspace string, reads, writes QueryEngine)

	ReserveConn bool
	SlowQueries bool

	VSchema         string
	VtGateExtraArgs []string

	wg sync.WaitGroup
}

func (bt *BufferingTest) Test(t *testing.T) {
	defer cluster.PanicHandler(t)
	clusterInstance, exitCode := bt.createCluster()
	if exitCode != 0 {
		t.Fatal("failed to start cluster")
	}
	defer clusterInstance.Teardown()

	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Insert two rows for the later threads (critical read, update).
	utils.Exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", criticalReadRowID, "'critical read'"))
	utils.Exec(t, conn, fmt.Sprintf("INSERT INTO buffer (id, msg) VALUES (%d, %s)", updateRowID, "'update'"))

	// Start both threads.
	readThreadInstance := &threadParams{
		index:               1,
		typ:                 "read",
		executeFunction:     readExecute,
		waitForNotification: make(chan bool),
		reservedConn:        bt.ReserveConn,
		slowQueries:         bt.SlowQueries,
	}
	bt.wg.Add(1)
	go readThreadInstance.threadRun(&bt.wg, &vtParams)
	updateThreadInstance := &threadParams{
		index:               1,
		typ:                 "write",
		executeFunction:     updateExecute,
		waitForNotification: make(chan bool),
		reservedConn:        bt.ReserveConn,
		slowQueries:         bt.SlowQueries,
	}
	bt.wg.Add(1)
	go updateThreadInstance.threadRun(&bt.wg, &vtParams)

	// Verify they got at least 2 RPCs through.
	readThreadInstance.ExpectQueries(2)
	updateThreadInstance.ExpectQueries(2)

	<-readThreadInstance.waitForNotification
	<-updateThreadInstance.waitForNotification

	bt.Failover(t, clusterInstance, keyspaceUnshardedName, readThreadInstance, updateThreadInstance)

	timeout := time.After(120 * time.Second)
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
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)

	var metadata VTGateBufferingStats
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(respByte, &metadata)
	require.NoError(t, err)

	label := fmt.Sprintf("%s.%s", keyspaceUnshardedName, "0")
	if metadata.BufferLastRequestsInFlightMax[label] == 0 {
		// Missed buffering is okay when we observed the failover during the
		// COMMIT (which cannot trigger the buffering).
		assert.Greater(t, updateThreadInstance.internalErrs, 0, "No buffering took place and the update thread saw no error during COMMIT. But one of it must happen.")
	} else {
		bt.Assert(t, label, &metadata)
	}

	bt.wg.Wait()
}

type VTGateBufferingStats struct {
	BufferLastRequestsInFlightMax map[string]int
	HealthcheckPrimaryPromoted    map[string]int
	BufferFailoverDurationSumMs   map[string]int
	BufferRequestsBuffered        map[string]int
	BufferStops                   map[string]int
}
