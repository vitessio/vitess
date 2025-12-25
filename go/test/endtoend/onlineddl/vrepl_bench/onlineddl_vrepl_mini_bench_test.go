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

package vreplstress

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/schema"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

type WriteMetrics struct {
	mu                                                      sync.Mutex
	insertsAttempts, insertsFailures, insertsNoops, inserts int64
	updatesAttempts, updatesFailures, updatesNoops, updates int64
	deletesAttempts, deletesFailures, deletesNoops, deletes int64
}

func (w *WriteMetrics) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.inserts = 0
	w.updates = 0
	w.deletes = 0

	w.insertsAttempts = 0
	w.insertsFailures = 0
	w.insertsNoops = 0

	w.updatesAttempts = 0
	w.updatesFailures = 0
	w.updatesNoops = 0

	w.deletesAttempts = 0
	w.deletesFailures = 0
	w.deletesNoops = 0
}

func (w *WriteMetrics) String() string {
	return fmt.Sprintf(`WriteMetrics: inserts-deletes=%d, updates-deletes=%d,
insertsAttempts=%d, insertsFailures=%d, insertsNoops=%d, inserts=%d,
updatesAttempts=%d, updatesFailures=%d, updatesNoops=%d, updates=%d,
deletesAttempts=%d, deletesFailures=%d, deletesNoops=%d, deletes=%d,
`,
		w.inserts-w.deletes, w.updates-w.deletes,
		w.insertsAttempts, w.insertsFailures, w.insertsNoops, w.inserts,
		w.updatesAttempts, w.updatesFailures, w.updatesNoops, w.updates,
		w.deletesAttempts, w.deletesFailures, w.deletesNoops, w.deletes,
	)
}

var (
	clusterInstance  *cluster.LocalProcessCluster
	shards           []cluster.Shard
	vtParams         mysql.ConnParams
	primaryTablet    *cluster.Vttablet
	replicaTablet    *cluster.Vttablet
	throttlerCheckOK atomic.Bool

	opOrder               int64
	opOrderMutex          sync.Mutex
	idSequence            atomic.Int32
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	cleanupStatements     = []string{
		`DROP TABLE IF EXISTS stress_test`,
		`DROP TABLE IF EXISTS t1`,
	}
	createStatement = `
		CREATE TABLE stress_test (
			id bigint not null,
			rand_val varchar(32) null default '',
			op_order bigint unsigned not null default 0,
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB;
		CREATE TABLE t1 (
			id bigint not null,
			i int not null default 0,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;
	`
	alterHintStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default '%s'
	`
	insertRowStatement = `
		INSERT IGNORE INTO stress_test (id, rand_val, op_order) VALUES (%d, left(md5(rand()), 8), %d)
	`
	updateRowStatement = `
		UPDATE stress_test SET op_order=%d, updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d AND updates=1
	`
	selectMaxOpOrder = `
		SELECT MAX(op_order) as m FROM stress_test
	`
	// We use CAST(SUM(updates) AS SIGNED) because SUM() returns a DECIMAL datatype, and we want to read a SIGNED INTEGER type
	selectCountRowsStatement = `
		SELECT COUNT(*) AS num_rows, CAST(SUM(updates) AS SIGNED) AS sum_updates FROM stress_test
	`
	truncateStatement = `
		TRUNCATE TABLE stress_test
	`
	writeMetrics WriteMetrics
)

var (
	countIterations = 1
)

const (
	maxTableRows         = 4096
	workloadDuration     = 45 * time.Second
	migrationWaitTimeout = 60 * time.Second
	// wlType               = insertBatchWorkloadType
	wlType           = mixedWorkloadType
	throttleWorkload = true
)

type workloadType int

const (
	mixedWorkloadType workloadType = iota
	insertWorkloadType
	insertBatchWorkloadType
)

func resetOpOrder() {
	opOrderMutex.Lock()
	defer opOrderMutex.Unlock()
	opOrder = 0
}

func nextOpOrder() int64 {
	opOrderMutex.Lock()
	defer opOrderMutex.Unlock()
	opOrder++
	return opOrder
}

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		schemaChangeDirectory = path.Join("/tmp", fmt.Sprintf("schema_change_dir_%d", clusterInstance.GetAndReserveTabletUID()))
		defer os.RemoveAll(schemaChangeDirectory)
		defer clusterInstance.Teardown()

		if _, err := os.Stat(schemaChangeDirectory); os.IsNotExist(err) {
			_ = os.Mkdir(schemaChangeDirectory, 0700)
		}

		clusterInstance.VtctldExtraArgs = []string{
			"--schema_change_dir", schemaChangeDirectory,
			"--schema_change_controller", "local",
			"--schema_change_check_interval", "1s",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			// "--relay_log_max_items", "10000000",
			// "--relay_log_max_size", "1000000000",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", fmt.Sprintf("%v", migrationWaitTimeout*2),
			"--migration_check_interval", "1.1s",
			"--watch_replication_stream",
			"--pprof-http",
			// Test VPlayer batching mode.
			fmt.Sprintf("--vreplication_experimental_flags=%d",
				// vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts),
				// vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching),
				vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching|vttablet.VReplicationExperimentalFlagVPlayerParallel),
		}
		clusterInstance.VtGateExtraArgs = []string{
			"--ddl_strategy", "online",
		}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		// No need for replicas in this stress test
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 0, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}

		primaryTablet = clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
		if len(clusterInstance.Keyspaces[0].Shards[0].Vttablets) > 1 {
			replicaTablet = clusterInstance.Keyspaces[0].Shards[0].Vttablets[1]
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

// trackVreplicationLag is used as a helper function to track vreplication lag and print progress to standard output.
func trackVreplicationLag(t *testing.T, ctx context.Context, workloadCtx context.Context, uuid string) {
	reportTicker := time.NewTicker(1 * time.Second)
	defer reportTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-workloadCtx.Done():
			return
		case <-reportTicker.C:
		}
		func() {
			query := fmt.Sprintf(`select time_updated, transaction_timestamp from _vt.vreplication where workflow='%s'`, uuid)
			rs, err := primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
			require.NoError(t, err)
			row := rs.Named().Row()
			if row == nil {
				return
			}

			durationDiff := func(t1, t2 time.Time) time.Duration {
				return t1.Sub(t2).Abs()
			}
			timeNow := time.Now()
			timeUpdated := time.Unix(row.AsInt64("time_updated", 0), 0)
			transactionTimestamp := time.Unix(row.AsInt64("transaction_timestamp", 0), 0)
			vreplicationLag := max(durationDiff(timeNow, timeUpdated), durationDiff(timeNow, transactionTimestamp))
			fmt.Printf("vreplication lag: %ds\n", int64(vreplicationLag.Seconds()))
		}()
	}
}

func TestVreplMiniStressSchemaChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)

	t.Run("validate config on primary", func(t *testing.T) {
		// Validate the config
		conn, err := primaryTablet.VttabletProcess.TabletConn(keyspaceName, true)
		require.NoError(t, err)
		defer conn.Close()

		_, err = conn.ExecuteFetch("set @@global.binlog_transaction_dependency_tracking='WRITESET'", 1, true)
		require.NoError(t, err)

		{
			rs, err := conn.ExecuteFetch("select @@global.binlog_transaction_dependency_tracking as v", 1, true)
			require.NoError(t, err)
			row := rs.Named().Row()
			require.NotNil(t, row)
			t.Logf("binlog_transaction_dependency_tracking: %v", row.AsString("v", ""))
		}
		_, err = conn.ExecuteFetch("set @@global.replica_preserve_commit_order=1", 1, true)
		require.NoError(t, err)
	})
	t.Run("validate config on replica", func(t *testing.T) {
		if replicaTablet == nil {
			t.SkipNow()
		}
		// Validate the config
		conn, err := replicaTablet.VttabletProcess.TabletConn(keyspaceName, true)
		require.NoError(t, err)
		defer conn.Close()
		{
			rs, err := conn.ExecuteFetch("select @@global.replica_parallel_workers as val", 1, true)
			require.NoError(t, err)
			row := rs.Named().Row()
			require.NotNil(t, row)
			parallelWorkers := row.AsInt64("val", 0)
			require.Positive(t, parallelWorkers)
			t.Logf("replica_parallel_workers: %v", parallelWorkers)
		}
		{
			rs, err := conn.ExecuteFetch("select @@global.replica_preserve_commit_order as val", 1, true)
			require.NoError(t, err)
			row := rs.Named().Row()
			require.NotNil(t, row)
			preserveCommitOrder := row.AsInt64("val", 0)
			require.Positive(t, preserveCommitOrder)
			t.Logf("replica_preserve_commit_order: %v", preserveCommitOrder)
		}
		_, err = conn.ExecuteFetch("set @@global.binlog_transaction_dependency_tracking='WRITESET'", 1, true)
		require.NoError(t, err)
	})

	throttlerCheckOK.Store(true)

	results := []time.Duration{}
	for i := 0; i < countIterations; i++ {
		// Finally, this is the real test:
		// We populate a table, and begin a concurrent workload (this is the "mini stress")
		// We then ALTER TABLE via vreplication.
		// Once convinced ALTER TABLE is complete, we stop the workload.
		// We then compare expected metrics with table metrics. If they agree, then
		// the vreplication/ALTER TABLE did not corrupt our data and we are happy.
		testName := fmt.Sprintf("ALTER TABLE with workload %d/%d", (i + 1), countIterations)
		t.Run(testName, func(t *testing.T) {
			t.Run("create schema", func(t *testing.T) {
				testWithInitialSchema(t)
			})
			t.Run("init table", func(t *testing.T) {
				initTable(t)
			})

			var uuid string
			t.Run("start migration", func(t *testing.T) {
				hint := fmt.Sprintf("hint-alter-with-workload-%d", i)
				uuid = testOnlineDDLStatement(t, fmt.Sprintf(alterHintStatement, hint), "online --postpone-completion --force-cut-over-after=1ns", "", true)
			})
			t.Run("wait for ready_to_complete", func(t *testing.T) {
				waitForReadyToComplete(t, uuid, true)
			})
			t.Run("throttle online-ddl", func(t *testing.T) {
				if !throttleWorkload {
					return
				}
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.VPlayerName, false)
				// onlineddl.ThrottleAllMigrations(t, &vtParams)

				appRule := &topodatapb.ThrottledAppRule{
					Name:      throttlerapp.VPlayerName.String(),
					Ratio:     throttle.DefaultThrottleRatio,
					ExpiresAt: protoutil.TimeToProto(time.Now().Add(time.Hour)),
				}
				req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
				_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
				assert.NoError(t, err)

				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.VPlayerName, true)
				throttler.WaitForCheckThrottlerResult(t, &clusterInstance.VtctldClientProcess, primaryTablet, throttlerapp.VPlayerName, nil, tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED, time.Minute)
			})
			readPos := func(t *testing.T) {
				{
					rs, err := primaryTablet.VttabletProcess.QueryTablet("select @@global.gtid_executed", keyspaceName, true)
					require.NoError(t, err)
					t.Logf("gtid executed: %v", rs.Rows[0][0].ToString())
				}
				{
					query := fmt.Sprintf("select pos from _vt.vreplication where workflow='%s'", uuid)
					rs, err := primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
					require.NoError(t, err)
					require.NotEmpty(t, rs.Rows)
					t.Logf("vreplication pos: %v", rs.Rows[0][0].ToString())
				}
			}
			t.Run("read pos", func(t *testing.T) {
				readPos(t)
			})
			t.Run(fmt.Sprintf("start workload: %v", workloadDuration), func(t *testing.T) {
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.VPlayerName, throttleWorkload)
				ctx, cancel := context.WithTimeout(ctx, workloadDuration)
				defer cancel()
				// runMultipleConnections(ctx, t, mixedWorkloadType)
				// runMultipleConnections(ctx, t, insertWorkloadType)
				runMultipleConnections(ctx, t)
			})
			t.Run("read pos", func(t *testing.T) {
				readPos(t)
			})
			t.Run("mark for completion", func(t *testing.T) {
				onlineddl.CheckCompleteAllMigrations(t, &vtParams, 1)
			})
			t.Run("validate throttler at end of workload", func(t *testing.T) {
				if !throttleWorkload {
					return
				}
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.VPlayerName, true)
				throttler.WaitForCheckThrottlerResult(t, &clusterInstance.VtctldClientProcess, primaryTablet, throttlerapp.VPlayerName, nil, tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED, time.Second)
			})
			var startTime = time.Now()
			t.Run("unthrottle online-ddl", func(t *testing.T) {
				if !throttleWorkload {
					return
				}
				// onlineddl.UnthrottleAllMigrations(t, &vtParams)

				appRule := &topodatapb.ThrottledAppRule{
					Name:      throttlerapp.VPlayerName.String(),
					ExpiresAt: protoutil.TimeToProto(time.Now()),
				}
				req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
				_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
				assert.NoError(t, err)

				if !onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.VPlayerName, false) {
					status, err := throttler.GetThrottlerStatus(&clusterInstance.VtctldClientProcess, primaryTablet)
					assert.NoError(t, err)

					t.Logf("Throttler status: %+v", status)
				}
			})
			t.Run("read pos", func(t *testing.T) {
				readPos(t)
			})
			t.Run("wait for migration to complete", func(t *testing.T) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				go trackVreplicationLag(t, ctx, ctx, uuid)

				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				t.Logf("# Migration status (for debug purposes): <%s>", status)
				if !onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete) {
					query := fmt.Sprintf("select * from _vt.vreplication where workflow='%s'", uuid)
					rs, err := primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, false)
					require.NoError(t, err)
					require.NotEmpty(t, rs.Rows)
					t.Logf("vreplication: %v", rs.Rows[0])
				}

				onlineddl.CheckCancelAllMigrations(t, &vtParams, -1)
			})
			t.Run("read pos", func(t *testing.T) {
				readPos(t)
			})
			endTime := time.Now()
			results = append(results, endTime.Sub(startTime))
			t.Logf(":::::::::::::::::::: Workload catchup took %v ::::::::::::::::::::", endTime.Sub(startTime))
			t.Run("cleanup", func(t *testing.T) {
				throttler.WaitForCheckThrottlerResult(t, &clusterInstance.VtctldClientProcess, primaryTablet, throttlerapp.VPlayerName, nil, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, time.Minute)
			})
			t.Run("validate metrics", func(t *testing.T) {
				testSelectTableMetrics(t)
			})
			t.Run("sleep", func(t *testing.T) {
				time.Sleep(time.Minute * 3)
			})
		})
	}

	t.Run("summary", func(t *testing.T) {
		t.Logf(":::::::::::::::::::: Workload catchup took: %+v ::::::::::::::::::::", results)
	})
}

func testWithInitialSchema(t *testing.T) {
	for _, statement := range cleanupStatements {
		err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, statement)
		require.NoError(t, err)
	}
	// Create the stress table
	err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, createStatement)
	require.NoError(t, err)

	// Check if table is created
	checkTable(t, tableName)
}

func waitForReadyToComplete(t *testing.T, uuid string, expected bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), migrationWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			readyToComplete := row.AsInt64("ready_to_complete", 0)
			if expected == (readyToComplete > 0) {
				// all good. This is what we waited for
				if expected {
					// if migration is ready to complete, the timestamp should be non-null
					assert.False(t, row["ready_to_complete_timestamp"].IsNull())
				} else {
					assert.True(t, row["ready_to_complete_timestamp"].IsNull())
				}
				return true
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			assert.NoError(t, ctx.Err(), "timeout waiting for ready_to_complete")
			return false
		}
	}
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, expectHint string, skipWait bool) (uuid string) {
	row := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, "").Named().Row()
	require.NotNil(t, row)
	uuid = row.AsString("uuid", "")
	uuid = strings.TrimSpace(uuid)
	require.NotEmpty(t, uuid)
	t.Logf("# Generated UUID (for debug purposes):")
	t.Logf("<%s>", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	if !strategySetting.Strategy.IsDirect() && !skipWait && uuid != "" {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		t.Logf("# Migration status (for debug purposes): <%s>", status)
	}

	if expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t *testing.T, showTableName string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, 1)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rowcount := 0

	for {
		queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
		require.NoError(t, err)
		rowcount = len(queryResult.Rows)
		if rowcount > 0 {
			break
		}

		select {
		case <-ticker.C:
			continue // Keep looping
		case <-ctx.Done():
			// Break below to the assertion
		}

		break
	}

	assert.Equal(t, expectCount, rowcount)
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectHint string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectHint)
	}
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), keyspaceName, true)
	require.NoError(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func generateInsert(t *testing.T, conn *mysql.Conn) error {
	id := idSequence.Add(1)
	if wlType != insertBatchWorkloadType {
		id = rand.Int32N(int32(maxTableRows))
	}
	query := fmt.Sprintf(insertRowStatement, id, nextOpOrder())
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.insertsAttempts++
		if err != nil {
			writeMetrics.insertsFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.insertsNoops++
			return
		}
		writeMetrics.inserts++
	}()
	{
		id := rand.Int32N(int32(maxTableRows))
		query := fmt.Sprintf("insert into t1 values (%d, %d)", id, id)
		_, _ = conn.ExecuteFetch(query, 1000, true)
	}
	return err
}

func generateUpdate(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, nextOpOrder(), id)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.updatesAttempts++
		if err != nil {
			writeMetrics.updatesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.updatesNoops++
			return
		}
		writeMetrics.updates++
	}()
	{
		id := rand.Int32N(int32(maxTableRows))
		query := fmt.Sprintf("update t1 set i=i+1 where id=%d", id)
		_, _ = conn.ExecuteFetch(query, 1000, true)
	}
	return err
}

func generateDelete(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, id)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics.mu.Lock()
		defer writeMetrics.mu.Unlock()

		writeMetrics.deletesAttempts++
		if err != nil {
			writeMetrics.deletesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics.deletesNoops++
			return
		}
		writeMetrics.deletes++
	}()
	{
		id := rand.Int32N(int32(maxTableRows))
		query := fmt.Sprintf("delete from t1 where id=%d", id)
		_, _ = conn.ExecuteFetch(query, 1000, true)
	}
	return err
}

func runSingleConnection(ctx context.Context, t *testing.T, sleepInterval time.Duration) {
	log.Infof("Running single connection")

	// conn, err := mysql.Connect(ctx, &vtParams)
	conn, err := primaryTablet.VttabletProcess.TabletConn(keyspaceName, true)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set innodb_lock_wait_timeout=5", 1000, true)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	require.NoError(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	require.NoError(t, err)

	ticker := time.NewTicker(sleepInterval)
	defer ticker.Stop()

	log.Infof("+- Starting single connection")
	defer log.Infof("+- Terminating single connection")
	for {
		select {
		case <-ctx.Done():
			log.Infof("runSingleConnection context timeout")
			return
		case <-ticker.C:
		}
		if !throttlerCheckOK.Load() {
			continue
		}
		switch wlType {
		case mixedWorkloadType:
			switch rand.Int32N(3) {
			case 0:
				err = generateInsert(t, conn)
			case 1:
				err = generateUpdate(t, conn)
			case 2:
				err = generateDelete(t, conn)
			}
			assert.Nil(t, err)
		case insertWorkloadType:
			err = generateInsert(t, conn)
			assert.Nil(t, err)
		case insertBatchWorkloadType:
			_, err := conn.ExecuteFetch("begin", 1, false)
			assert.Nil(t, err)
			for range 50 {
				err = generateInsert(t, conn)
				assert.Nil(t, err)
			}
			_, err = conn.ExecuteFetch("commit", 1, false)
			assert.Nil(t, err)
		}
	}
}

func runMultipleConnections(ctx context.Context, t *testing.T) {
	// The workload for a 16 vCPU machine is:
	// - Concurrency of 16
	// - 2ms interval between queries for each connection
	// As the number of vCPUs decreases, so do we decrease concurrency, and increase intervals. For example, on a 8 vCPU machine
	// we run concurrency of 8 and interval of 4ms. On a 4 vCPU machine we run concurrency of 4 and interval of 8ms.
	maxConcurrency := runtime.NumCPU()

	{
		// Unlike similar stress tests, here we're not looking to be any mercyful. We're not looking to have
		// replication capacity. We want to hammer as much as possible, then wait for catchup time. We therefore
		// use a minimal sleep interval.
		//
		// sleepModifier := 16.0 / float64(maxConcurrency)
		// baseSleepInterval := 2 * time.Millisecond
		// singleConnectionSleepIntervalNanoseconds := float64(baseSleepInterval.Nanoseconds()) * sleepModifier
		// sleepInterval := time.Duration(int64(singleConnectionSleepIntervalNanoseconds))
	}
	sleepInterval := time.Microsecond

	log.Infof("Running multiple connections: maxConcurrency=%v, sleep interval=%v", maxConcurrency, sleepInterval)
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, sleepInterval)
		}()
	}
	flushBinlogs := func() {
		// Flushing binlogs on primary restarts imposes more challenges to parallel vplayer
		// because a binlog rotation is a parallellism barrier: all events from previous binlog
		// must be consumed before starting to apply events in new binlog.
		conn, err := primaryTablet.VttabletProcess.TabletConn(keyspaceName, true)
		require.NoError(t, err)
		defer conn.Close()

		_, err = conn.ExecuteFetch("flush binary logs", 1000, true)
		require.NoError(t, err)
	}
	time.AfterFunc(200*time.Millisecond, flushBinlogs)
	time.AfterFunc(400*time.Millisecond, flushBinlogs)
	wg.Wait()
	log.Infof("Running multiple connections: done")
}

func initTable(t *testing.T) {
	log.Infof("initTable begin")
	defer log.Infof("initTable complete")

	t.Run("cancel pending migrations", func(t *testing.T) {
		cancelQuery := "alter vitess_migration cancel all"
		r := onlineddl.VtgateExecQuery(t, &vtParams, cancelQuery, "")
		if r.RowsAffected > 0 {
			fmt.Printf("# Cancelled migrations (for debug purposes): %d\n", r.RowsAffected)
		}
	})

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	resetOpOrder()
	writeMetrics.Clear()
	_, err = conn.ExecuteFetch(truncateStatement, 1000, true)
	require.NoError(t, err)

	for i := 0; i < maxTableRows/2; i++ {
		generateInsert(t, conn)
	}
	if wlType != insertBatchWorkloadType {
		for i := 0; i < maxTableRows/4; i++ {
			generateUpdate(t, conn)
		}
		for i := 0; i < maxTableRows/4; i++ {
			generateDelete(t, conn)
		}
	}
}

func testSelectTableMetrics(t *testing.T) {
	writeMetrics.mu.Lock()
	defer writeMetrics.mu.Unlock()

	{
		rs := onlineddl.VtgateExecQuery(t, &vtParams, selectMaxOpOrder, "")
		row := rs.Named().Row()
		require.NotNil(t, row)

		maxOpOrder := row.AsInt64("m", 0)
		fmt.Printf("# max op_order in table: %d\n", maxOpOrder)
	}

	log.Infof("%s", writeMetrics.String())

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	rs, err := conn.ExecuteFetch(selectCountRowsStatement, 1000, true)
	require.NoError(t, err)

	row := rs.Named().Row()
	require.NotNil(t, row)
	log.Infof("testSelectTableMetrics, row: %v", row)
	numRows := row.AsInt64("num_rows", 0)
	sumUpdates := row.AsInt64("sum_updates", 0)
	assert.NotZero(t, numRows)
	assert.NotZero(t, sumUpdates)
	assert.NotZero(t, writeMetrics.inserts)
	assert.NotZero(t, writeMetrics.deletes)
	assert.NotZero(t, writeMetrics.updates)
	assert.Equal(t, writeMetrics.inserts-writeMetrics.deletes, numRows)
	assert.Equal(t, writeMetrics.updates-writeMetrics.deletes, sumUpdates) // because we DELETE WHERE updates=1
}
