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

// This test is designed to test the flow of a single online DDL migration, with tablet throttler
// enabled. IT tests the following:
// - A primary + replica setup
// - Creating and populating a table
// - Enabling tablet (lag) throttler
// - Running a workload that generates DMLs, and which checks the throttler
// - Running an online DDL migration:
//   - Using `online --postpone-completion` to use vreplication
//   - vreplication configured (by default) to read from replica
//   - vreplication by nature also checks the throttler
//   - meanwhile, the workload generates DMLs, give migration some run time
//   - proactively throttle and then unthrottle the migration
//   - complete the migration
//
// - Validate sufficient DML has been applied
// - Validate the migration completed, and validate new schema is instated
//
// The test is designed with upgrade/downgrade in mind. In particular, we wish to test
// different vitess versions for `primary` and `replica` tablets. Thus, we validate:
// - Cross tablet and cross version throttler communication
// - Cross version vreplication

package flow

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
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
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vttablet"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	clusterInstance  *cluster.LocalProcessCluster
	shards           []cluster.Shard
	vtParams         mysql.ConnParams
	primaryTablet    *cluster.Vttablet
	replicaTablet    *cluster.Vttablet
	tablets          []*cluster.Vttablet
	httpClient       = throttlebase.SetupHTTPClient(time.Second)
	throttleWorkload atomic.Bool
	totalAppliedDML  atomic.Int64

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	createStatement       = `
		CREATE TABLE stress_test (
			id bigint(20) not null,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
	`
	alterHintStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default '%s'
	`
	insertRowStatement = `
		INSERT IGNORE INTO stress_test (id, rand_val) VALUES (%d, left(md5(rand()), 8))
	`
	updateRowStatement = `
		UPDATE stress_test SET updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d AND updates=1
	`
)

var (
	countIterations = 5
)

const (
	maxTableRows         = 4096
	workloadDuration     = 5 * time.Second
	migrationWaitTimeout = 60 * time.Second
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
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
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "2s",
			"--watch_replication_stream",
			// Test VPlayer batching mode.
			fmt.Sprintf("--vreplication_experimental_flags=%d",
				vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching),
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
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
		}

		// Collect table paths and ports
		tablets = clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = tablet
			} else {
				replicaTablet = tablet
			}
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

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	require.NotNil(t, clusterInstance)
	require.NotNil(t, primaryTablet)
	require.NotNil(t, replicaTablet)
	require.Equal(t, 2, len(tablets))

	// This test is designed with upgrade/downgrade in mind. Do some logging to show what's
	// the configuration for this test.
	if binarySuffix := os.Getenv("PRIMARY_TABLET_BINARY_SUFFIX"); binarySuffix != "" {
		t.Logf("Using PRIMARY_TABLET_BINARY_SUFFIX: %s", binarySuffix)
	}
	if binarySuffix := os.Getenv("REPLICA_TABLET_BINARY_SUFFIX"); binarySuffix != "" {
		t.Logf("Using REPLICA_TABLET_BINARY_SUFFIX: %s", binarySuffix)
	}

	require.NotEmpty(t, clusterInstance.Keyspaces)
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)

	t.Run("flow", func(t *testing.T) {
		t.Run("create schema", func(t *testing.T) {
			testWithInitialSchema(t)
		})
		t.Run("init table", func(t *testing.T) {
			// Populates table. Makes work for vcopier.
			initTable(t)
		})
		t.Run("migrate", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			workloadCtx, cancelWorkload := context.WithCancel(ctx)
			defer cancelWorkload()

			t.Run("routine throttler check", func(t *testing.T) {
				go func() {
					ticker := time.NewTicker(500 * time.Millisecond)
					defer ticker.Stop()
					for {
						_, statusCode, err := throttlerCheck(primaryTablet.VttabletProcess, throttlerapp.OnlineDDLName)
						assert.NoError(t, err)
						throttleWorkload.Store(statusCode != http.StatusOK)
						select {
						case <-ticker.C:
						case <-workloadCtx.Done():
							t.Logf("Terminating routine throttler check")
							return
						}
					}
				}()
			})

			var wg sync.WaitGroup
			t.Run("generate workload", func(t *testing.T) {
				// Create work for vplayer.
				// This workload will consider throttling state and avoid generating DMLs if throttled.
				wg.Add(1)
				go func() {
					defer cancel()
					defer t.Logf("Terminating workload")
					defer wg.Done()
					runMultipleConnections(workloadCtx, t)
				}()
			})
			appliedDMLStart := totalAppliedDML.Load()

			hint := "post_completion_hint"
			var uuid string
			t.Run("submit migration", func(t *testing.T) {
				uuid = testOnlineDDLStatement(t, fmt.Sprintf(alterHintStatement, hint), "online --postpone-completion", "", true)
			})
			t.Run("wait for ready_to_complete", func(t *testing.T) {
				waitForReadyToComplete(t, uuid, true)
			})
			t.Run("validating running status", func(t *testing.T) {
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
			})
			t.Run("throttle online-ddl", func(t *testing.T) {
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, false)
				onlineddl.ThrottleAllMigrations(t, &vtParams)
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, true)
				waitForThrottleCheckStatus(t, throttlerapp.OnlineDDLName, primaryTablet, http.StatusExpectationFailed)
			})
			t.Run("unthrottle online-ddl", func(t *testing.T) {
				onlineddl.UnthrottleAllMigrations(t, &vtParams)
				if !onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, false) {
					status, err := throttler.GetThrottlerStatus(&clusterInstance.VtctldClientProcess, primaryTablet)
					assert.NoError(t, err)

					t.Logf("Throttler status: %+v", status)
				}
				waitForThrottleCheckStatus(t, throttlerapp.OnlineDDLName, primaryTablet, http.StatusOK)
			})
			t.Run("apply more DML", func(t *testing.T) {
				// Looking to run a substantial amount of DML, giving vreplication
				// more "opportunities" to throttle or to make progress.
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()

				startDML := totalAppliedDML.Load()
				for {
					appliedDML := totalAppliedDML.Load()
					if appliedDML-startDML >= int64(maxTableRows) {
						// We have generated enough DMLs
						return
					}
					select {
					case <-ticker.C:
					case <-ctx.Done():
						require.Fail(t, "timeout waiting for applied DML")
					}
				}
			})
			t.Run("validate applied DML", func(t *testing.T) {
				// Validate that during Online DDL, and even with throttling, we were
				// able to produce meaningful traffic.
				appliedDMLEnd := totalAppliedDML.Load()
				assert.Greater(t, appliedDMLEnd, appliedDMLStart)
				assert.GreaterOrEqual(t, appliedDMLEnd-appliedDMLStart, int64(maxTableRows))
				t.Logf("Applied DML: %d", appliedDMLEnd-appliedDMLStart)
			})
			t.Run("attempt to complete", func(t *testing.T) {
				onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
			})
			isComplete := false
			t.Run("optimistic wait for migration completion", func(t *testing.T) {
				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
				isComplete = (status == schema.OnlineDDLStatusComplete)
				t.Logf("# Migration status (for debug purposes): <%s>", status)
			})
			if !isComplete {
				t.Run("force complete cut-over", func(t *testing.T) {
					onlineddl.CheckForceMigrationCutOver(t, &vtParams, shards, uuid, true)
				})
				t.Run("another optimistic wait for migration completion", func(t *testing.T) {
					status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
					isComplete = (status == schema.OnlineDDLStatusComplete)
					t.Logf("# Migration status (for debug purposes): <%s>", status)
				})
			}
			if !isComplete {
				t.Run("terminate workload", func(t *testing.T) {
					// Seems like workload is too high and preventing migration from completing.
					// We can't go on forever. It's nice to have normal completion under workload,
					// but it's not strictly what this test is designed for. We terminate the
					// workload so as to allow the migration to complete.
					cancelWorkload()
				})
			}
			t.Run("wait for migration completion", func(t *testing.T) {
				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
				t.Logf("# Migration status (for debug purposes): <%s>", status)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			})
			t.Run("validate table schema", func(t *testing.T) {
				checkMigratedTable(t, tableName, hint)
			})

			cancelWorkload() // Early break
			cancel()         // Early break
			wg.Wait()
		})
	})
}

func testWithInitialSchema(t *testing.T) {
	// Create the stress table
	err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, createStatement)
	require.Nil(t, err)

	// Check if table is created
	checkTable(t, tableName)
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
		require.Nil(t, err)
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
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
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

func generateInsert(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, id)
	_, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	return err
}

func generateUpdate(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, id)
	_, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	return err
}

func generateDelete(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, id)
	_, err := conn.ExecuteFetch(query, 1, false)
	if err == nil {
		totalAppliedDML.Add(1)
	}

	return err
}

func runSingleConnection(ctx context.Context, t *testing.T, sleepInterval time.Duration) {
	log.Infof("Running single connection")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	require.Nil(t, err)

	ticker := time.NewTicker(sleepInterval)
	defer ticker.Stop()

	for {
		if !throttleWorkload.Load() {
			switch rand.Int32N(3) {
			case 0:
				err = generateInsert(t, conn)
			case 1:
				err = generateUpdate(t, conn)
			case 2:
				err = generateDelete(t, conn)
			}
		}
		select {
		case <-ctx.Done():
			log.Infof("Terminating single connection")
			return
		case <-ticker.C:
		}
		assert.Nil(t, err)
	}
}

func runMultipleConnections(ctx context.Context, t *testing.T) {
	// The workload for a 16 vCPU machine is:
	// - Concurrency of 16
	// - 2ms interval between queries for each connection
	// As the number of vCPUs decreases, so do we decrease concurrency, and increase intervals. For example, on a 8 vCPU machine
	// we run concurrency of 8 and interval of 4ms. On a 4 vCPU machine we run concurrency of 4 and interval of 8ms.
	maxConcurrency := runtime.NumCPU()
	sleepModifier := 16.0 / float64(maxConcurrency)
	baseSleepInterval := 2 * time.Millisecond
	singleConnectionSleepIntervalNanoseconds := float64(baseSleepInterval.Nanoseconds()) * sleepModifier
	sleepInterval := time.Duration(int64(singleConnectionSleepIntervalNanoseconds))

	log.Infof("Running multiple connections: maxConcurrency=%v, sleep interval=%v", maxConcurrency, sleepInterval)
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, sleepInterval)
		}()
	}
	wg.Wait()
	log.Infof("Running multiple connections: done")
}

func initTable(t *testing.T) {
	log.Infof("initTable begin")
	defer log.Infof("initTable complete")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	appliedDMLStart := totalAppliedDML.Load()

	for i := 0; i < maxTableRows/2; i++ {
		generateInsert(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateUpdate(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateDelete(t, conn)
	}
	appliedDMLEnd := totalAppliedDML.Load()
	assert.Greater(t, appliedDMLEnd, appliedDMLStart)
	assert.GreaterOrEqual(t, appliedDMLEnd-appliedDMLStart, int64(maxTableRows))
}

func throttleResponse(tablet *cluster.VttabletProcess, path string) (respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/%s", tablet.TabletHostname, tablet.Port, path)
	resp, err := httpClient.Get(apiURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return respBody, err
}

func throttleApp(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/throttle-app?app=%s&duration=1h", throttlerApp.String()))
}

func unthrottleApp(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/unthrottle-app?app=%s", throttlerApp.String()))
}

func throttlerCheck(tablet *cluster.VttabletProcess, throttlerApp throttlerapp.Name) (respBody string, statusCode int, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/throttler/check?app=%s", tablet.TabletHostname, tablet.Port, throttlerApp.String())
	resp, err := httpClient.Get(apiURL)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	statusCode = resp.StatusCode
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return respBody, statusCode, err
}

// waitForThrottleCheckStatus waits for the tablet to return the provided HTTP code in a throttle check
func waitForThrottleCheckStatus(t *testing.T, throttlerApp throttlerapp.Name, tablet *cluster.Vttablet, wantCode int) {
	ctx, cancel := context.WithTimeout(context.Background(), migrationWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		respBody, statusCode, err := throttlerCheck(tablet.VttabletProcess, throttlerApp)
		require.NoError(t, err)

		if wantCode == statusCode {
			return
		}
		select {
		case <-ctx.Done():
			assert.Equalf(t, wantCode, statusCode, "body: %s", respBody)
			return
		case <-ticker.C:
		}
	}
}
