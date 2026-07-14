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
	"math/rand/v2"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterInstance  *vitesst.Cluster
	shards           []*vitesst.Shard
	vtParams         mysql.ConnParams
	primaryTablet    *vitesst.Tablet
	replicaTablet    *vitesst.Tablet
	tablets          []*vitesst.Tablet
	throttleWorkload atomic.Bool
	totalAppliedDML  atomic.Int64

	keyspaceName          = "ks"
	schemaChangeDirectory = "/vt/files"
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

var countIterations = 5

const (
	maxTableRows           = 4096
	workloadDuration       = 5 * time.Second
	migrationWaitTimeout   = 60 * time.Second
	throttlerConfigTimeout = 60 * time.Second
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithVTCtldArgs(
				"--schema-change-dir", schemaChangeDirectory,
				"--schema-change-controller", "local",
				"--schema-change-check-interval", "1s",
			),
			vitesst.WithVTTabletArgs(
				"--heartbeat-interval", "250ms",
				"--heartbeat-on-demand-duration", "5s",
				"--migration-check-interval", "2s",
			),
			vitesst.WithVTGateArgs(
				"--ddl-strategy", "online",
			),
			// No need for replicas in this stress test
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("1").
				WithReplicas(1),
		)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		clusterInstance = cluster

		// Collect table paths and ports
		tablets = cluster.Keyspace(keyspaceName).Tablets()
		for _, tablet := range tablets {
			if tablet.Type() == "primary" {
				primaryTablet = tablet
			} else {
				replicaTablet = tablet
			}
		}

		vtParams = cluster.VTParams(ctx, "")

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestOnlineDDLFlow(t *testing.T) {
	ctx := t.Context()

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

	require.NotEmpty(t, clusterInstance.Keyspaces())
	shards = clusterInstance.Keyspace(keyspaceName).Shards()
	require.Equal(t, 1, len(shards))

	enableLagThrottlerAndWaitForStatus(t)

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
						resp, err := checkThrottler(context.WithoutCancel(workloadCtx), primaryTablet, throttlerapp.OnlineDDLName)
						if !assert.NoError(t, err) {
							return
						}
						throttleWorkload.Store(resp.Check.ResponseCode != tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
						select {
						case <-ticker.C:
						case <-workloadCtx.Done():
							fmt.Println("Terminating routine throttler check")
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
					defer wg.Done()
					defer fmt.Println("Terminating workload")
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
				checkMigrationStatus(t, uuid, schema.OnlineDDLStatusRunning)
			})
			t.Run("throttle online-ddl", func(t *testing.T) {
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, false)
				onlineddl.ThrottleAllMigrations(t, &vtParams)
				onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, true)
				waitForCheckThrottlerResult(t, primaryTablet, throttlerapp.OnlineDDLName, tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED, migrationWaitTimeout)
			})
			t.Run("unthrottle online-ddl", func(t *testing.T) {
				onlineddl.UnthrottleAllMigrations(t, &vtParams)
				if !onlineddl.CheckThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, false) {
					status, err := getThrottlerStatus(t.Context(), primaryTablet)
					assert.NoError(t, err)

					t.Logf("Throttler status: %+v", status)
				}
				waitForCheckThrottlerResult(t, primaryTablet, throttlerapp.OnlineDDLName, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, migrationWaitTimeout)
			})
			t.Run("apply more DML", func(t *testing.T) {
				// Looking to run a substantial amount of DML, giving vreplication
				// more "opportunities" to throttle or to make progress.
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
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
				checkCompleteMigration(t, uuid, true)
			})
			isComplete := false
			t.Run("optimistic wait for migration completion", func(t *testing.T) {
				status := waitForMigrationStatus(t, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
				isComplete = (status == schema.OnlineDDLStatusComplete)
				t.Logf("# Migration status (for debug purposes): <%s>", status)
			})
			if !isComplete {
				t.Run("force complete cut-over", func(t *testing.T) {
					checkForceMigrationCutOver(t, uuid, true)
				})
				t.Run("another optimistic wait for migration completion", func(t *testing.T) {
					status := waitForMigrationStatus(t, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
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
				status := waitForMigrationStatus(t, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete)
				t.Logf("# Migration status (for debug purposes): <%s>", status)
				checkMigrationStatus(t, uuid, schema.OnlineDDLStatusComplete)
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

// updateThrottlerConfig runs vtctldclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func updateThrottlerConfig(ctx context.Context, args ...string) (result string, err error) {
	args = append([]string{"UpdateThrottlerConfig"}, args...)
	args = append(args, "--custom-query", "")
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// checkThrottler runs vtctldclient CheckThrottler for the given app, in self scope.
func checkThrottler(ctx context.Context, tablet *vitesst.Tablet, appName throttlerapp.Name) (*vtctldatapb.CheckThrottlerResponse, error) {
	args := []string{"CheckThrottler"}
	if appName != "" {
		args = append(args, "--app-name", appName.String())
	}
	args = append(args, "--scope", throttlebase.SelfScope.String())
	args = append(args, "--request-heartbeats")
	args = append(args, tablet.Alias())

	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.CheckThrottlerResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// getThrottlerStatus runs vtctldclient GetThrottlerStatus.
func getThrottlerStatus(ctx context.Context, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tablet.Alias())
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// tabletNotServing indicates whether the tablet reports itself as not serving, in which case
// its throttler is not open and its throttler status is not indicative.
func tabletNotServing(ctx context.Context, tablet *vitesst.Tablet) bool {
	_, body, err := tablet.MakeAPICall(ctx, "/debug/status_details")
	if err != nil {
		return false
	}
	class := strings.ToLower(gjson.Get(body, "0.Class").String())
	value := strings.ToLower(gjson.Get(body, "0.Value").String())
	return class == "unhappy" && strings.Contains(value, "not serving")
}

// waitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled until the specified timeout.
func waitForThrottlerStatusEnabled(t *testing.T, tablet *vitesst.Tablet, enabled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if tabletNotServing(ctx, tablet) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
			return
		}

		status, err := getThrottlerStatus(ctx, tablet)
		good := func() bool {
			if err != nil {
				log.Error(fmt.Sprintf("GetThrottlerStatus failed: %v", err))
				return false
			}
			if status.IsEnabled != enabled {
				return false
			}
			if status.IsEnabled && len(status.MetricsHealth) == 0 {
				// throttler is enabled, but no metrics collected yet. Wait for something to be collected.
				return false
			}
			return true
		}
		if good() {
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttler status enabled to be %t with the correct config after %v; last seen status: %+v",
				tablet.Alias(), enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// enableLagThrottlerAndWaitForStatus enables the throttler, configured to use the standard
// replication lag metric. The function waits until the throttler is confirmed to be running
// on all tablets.
func enableLagThrottlerAndWaitForStatus(t *testing.T) {
	_, err := updateThrottlerConfig(t.Context(), "--enable")
	require.NoError(t, err)

	for _, tablet := range tablets {
		waitForThrottlerStatusEnabled(t, tablet, true, time.Minute)
	}
}

// waitForCheckThrottlerResult waits for the tablet's throttler to return the expected response code
func waitForCheckThrottlerResult(t *testing.T, tablet *vitesst.Tablet, appName throttlerapp.Name, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, bool) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := checkThrottler(context.WithoutCancel(ctx), tablet, appName)
		require.NoError(t, err)
		if resp.Check.ResponseCode == wantCode {
			return resp, true
		}
		select {
		case <-ctx.Done():
			assert.Failf(t, "timeout", "waiting for %s tablet's throttler to return a %v check result after %v; last seen value: %+v", tablet.Alias(), wantCode, timeout, resp.Check.ResponseCode)
			return resp, false
		case <-ticker.C:
		}
	}
}

// checkCompleteMigration attempts to complete a migration, and expects success/failure by counting affected rows
func checkCompleteMigration(t *testing.T, uuid string, expectCompletePossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a complete",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := onlineddl.VtgateExecQuery(t, &vtParams, query, "")

	if expectCompletePossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkForceMigrationCutOver forces a migration to cut over, and expects success/failure by counting affected rows
func checkForceMigrationCutOver(t *testing.T, uuid string, expectPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a force_cutover",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := onlineddl.VtgateExecQuery(t, &vtParams, query, "")

	if expectPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func checkMigrationStatus(t *testing.T, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	query, err := sqlparser.ParseAndBind(fmt.Sprintf("show vitess_migrations from %s like %%a", keyspaceName),
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := onlineddl.VtgateExecQuery(t, &vtParams, query, "")
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

// waitForMigrationStatus waits for a migration to reach either provided statuses (returns immediately), or eventually time out
func waitForMigrationStatus(t *testing.T, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard.Name] = true
	}
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastKnownStatus := ""
	for {
		countMatchedShards := 0
		r := onlineddl.VtgateExecQuery(t, &vtParams, query, "")
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
		select {
		case <-ctx.Done():
			return schema.OnlineDDLStatus(lastKnownStatus)
		case <-ticker.C:
		}
	}
}

func testWithInitialSchema(t *testing.T) {
	// Create the stress table
	err := clusterInstance.Vtctld().ExecuteCommand(t.Context(),
		"ApplySchema",
		"--sql", createStatement,
		"--ddl-strategy", "direct -allow-zero-in-date",
		keyspaceName,
	)
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
		status := waitForMigrationStatus(t, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		t.Logf("# Migration status (for debug purposes): <%s>", status)
	}

	if expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t *testing.T, showTableName string) {
	for _, shard := range clusterInstance.Keyspace(keyspaceName).Shards() {
		checkTablesCount(t, shard.Primary(), showTableName, 1)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *vitesst.Tablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	rowcount := 0

	for {
		queryResult, err := tablet.QueryTablet(ctx, query)
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
	for _, shard := range clusterInstance.Keyspace(keyspaceName).Shards() {
		createStatement := getCreateTableStatement(t, shard.Primary(), tableName)
		assert.Contains(t, createStatement, expectHint)
	}
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *vitesst.Tablet, tableName string) (statement string) {
	queryResult, err := tablet.QueryTablet(t.Context(), fmt.Sprintf("show create table %s;", tableName))
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func waitForReadyToComplete(t *testing.T, uuid string, expected bool) bool {
	ctx, cancel := context.WithTimeout(t.Context(), migrationWaitTimeout)
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
	log.Info("Running single connection")
	conn, err := mysql.Connect(ctx, &vtParams)
	if !assert.Nil(t, err) {
		return
	}
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	if !assert.Nil(t, err) {
		return
	}
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	if !assert.Nil(t, err) {
		return
	}

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
			log.Info("Terminating single connection")
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

	log.Info(fmt.Sprintf("Running multiple connections: maxConcurrency=%v, sleep interval=%v", maxConcurrency, sleepInterval))
	var wg sync.WaitGroup
	for range maxConcurrency {
		wg.Go(func() {
			runSingleConnection(ctx, t, sleepInterval)
		})
	}
	wg.Wait()
	log.Info("Running multiple connections: done")
}

func initTable(t *testing.T) {
	log.Info("initTable begin")
	defer log.Info("initTable complete")

	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	appliedDMLStart := totalAppliedDML.Load()

	for range maxTableRows / 2 {
		generateInsert(t, conn)
	}
	for range maxTableRows / 4 {
		generateUpdate(t, conn)
	}
	for range maxTableRows / 4 {
		generateDelete(t, conn)
	}
	appliedDMLEnd := totalAppliedDML.Load()
	assert.Greater(t, appliedDMLEnd, appliedDMLStart)
	assert.GreaterOrEqual(t, appliedDMLEnd-appliedDMLStart, int64(maxTableRows))
}
