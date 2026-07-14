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

package vrepl

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterInstance *vitesst.Cluster
	shards          []*vitesst.Shard
	vtParams        mysql.ConnParams

	normalMigrationWait   = 45 * time.Second
	extendedMigrationWait = 60 * time.Second

	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = "/vt/files"
	totalTableCount       = 4
	createTable           = `
		CREATE TABLE %s (
			id bigint(20) NOT NULL,
			test_val bigint unsigned NOT NULL DEFAULT 0,
			msg varchar(64),
			PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	// To verify non online-DDL behavior
	alterTableNormalStatement = `
		ALTER TABLE %s
			ADD COLUMN non_online int UNSIGNED NOT NULL DEFAULT 0`
	// A trivial statement which must succeed and does not change the schema
	alterTableTrivialStatement = `
		ALTER TABLE %s
			ENGINE=InnoDB`
	// The following statement is valid
	alterTableSuccessfulStatement = `
		ALTER TABLE %s
			MODIFY id bigint UNSIGNED NOT NULL,
			ADD COLUMN vrepl_col int NOT NULL DEFAULT 0,
			ADD INDEX idx_msg(msg)`
	// The following statement will fail because vreplication requires shared PRIMARY KEY columns
	alterTableFailedStatement = `
		ALTER TABLE %s
			DROP PRIMARY KEY,
			DROP COLUMN vrepl_col`
	alterTableFailedVreplicationStatement = `
			ALTER TABLE %s
				ADD UNIQUE KEY test_val_uidx (test_val)`
	// We will run this query while throttling vreplication
	alterTableThrottlingStatement = `
		ALTER TABLE %s
			DROP COLUMN vrepl_col`
	onlineDDLCreateTableStatement = `
		CREATE TABLE %s (
			id bigint NOT NULL,
			test_val bigint unsigned NOT NULL DEFAULT 0,
			online_ddl_create_col INT NOT NULL DEFAULT 0,
			PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	onlineDDLDropTableStatement = `
		DROP TABLE %s`
	onlineDDLDropTableIfExistsStatement = `
		DROP TABLE IF EXISTS %s`
	insertRowStatement = `
		INSERT INTO %s (id, test_val) VALUES (%d, 1)
	`
	selectCountRowsStatement = `
		SELECT COUNT(*) AS c FROM %s
	`
	countInserts int64
	insertMutex  sync.Mutex

	vSchema = `
	{
		"sharded": true,
		"vindexes": {
			"hash_index": {
				"type": "hash"
			}
		},
		"tables": {
			"vt_onlineddl_test_00": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			},
			"vt_onlineddl_test_01": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			},
			"vt_onlineddl_test_02": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			},
			"vt_onlineddl_test_03": {
				"column_vindexes": [
					{
						"column": "id",
						"name": "hash_index"
					}
				]
			}
		}
	}
	`
)

const (
	customThreshold         = 5
	throttlerEnabledTimeout = 60 * time.Second
	vtgateHealthyTimeout    = 2 * time.Minute
	noCustomQuery           = ""
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode := func() int {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell),
			vitesst.WithVTCtldArgs(
				"--schema-change-dir", schemaChangeDirectory,
				"--schema-change-controller", "local",
				"--schema-change-check-interval", "1s",
			),
			vitesst.WithVTTabletArgs(
				"--heartbeat-interval", "250ms",
				"--migration-check-interval", "5s",
			),
			vitesst.WithVTGateArgs(
				"--ddl-strategy", "online",
			),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames("-80", "80-").
				WithReplicas(1).
				WithVSchema(vSchema),
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
		vtParams = cluster.VTParams(ctx, "")

		return m.Run()
	}()
	os.Exit(exitcode)
}

// waitForTabletsToHealthyInVtgate waits until vtgate's healthcheck has a serving
// connection to each of the cluster's tablets: one primary per shard, plus the
// shard's replica and rdonly tablets.
func waitForTabletsToHealthyInVtgate(ctx context.Context) error {
	for _, ks := range clusterInstance.Keyspaces() {
		for _, shard := range ks.Shards() {
			expected := map[string]float64{
				fmt.Sprintf("%s.%s.primary", ks.Name, shard.Name): 1,
			}
			if count := len(shard.Replicas()); count > 0 {
				expected[fmt.Sprintf("%s.%s.replica", ks.Name, shard.Name)] = float64(count)
			}
			if count := len(shard.RDOnly()); count > 0 {
				expected[fmt.Sprintf("%s.%s.rdonly", ks.Name, shard.Name)] = float64(count)
			}
			_, _, err := clusterInstance.VTGate().MakeAPICallRetry(ctx, "/debug/vars", vtgateHealthyTimeout,
				func(status int, body string) bool {
					if status != 200 {
						return false
					}
					var vars struct {
						HealthcheckConnections map[string]float64 `json:"HealthcheckConnections"`
					}
					if err := json.Unmarshal([]byte(body), &vars); err != nil {
						return false
					}
					for name, count := range expected {
						if vars.HealthcheckConnections[name] != count {
							return false
						}
					}
					return true
				})
			if err != nil {
				return fmt.Errorf("wait for healthy tablets of %s failed: %w", shard.Ref(), err)
			}
		}
	}
	return nil
}

func TestVreplSchemaChanges(t *testing.T) {
	shards = clusterInstance.Keyspace(keyspaceName).Shards()
	require.Equal(t, 2, len(shards))
	for _, shard := range shards {
		require.Equal(t, 2, len(shard.Tablets()))
	}
	shardTablets := shards[0].Tablets()

	providedUUID := ""
	providedMigrationContext := ""

	// We execute the throttler commands via vtgate, which in turn
	// executes them via vttablet. So let's wait until vtgate's view
	// is updated.
	err := waitForTabletsToHealthyInVtgate(t.Context())
	require.NoError(t, err)

	t.Run("WaitForSrvKeyspace", func(t *testing.T) {
		for _, ks := range clusterInstance.Keyspaces() {
			t.Run(ks.Name, func(t *testing.T) {
				err := waitForSrvKeyspace(t.Context(), cell, ks.Name)
				require.NoError(t, err)
			})
		}
	})
	t.Run("updating throttler config", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true, Threshold: customThreshold}
		_, err := updateThrottlerTopoConfig(t.Context(), req, nil, nil)
		require.NoError(t, err)
	})

	t.Run("checking throttler config", func(t *testing.T) {
		for _, ks := range clusterInstance.Keyspaces() {
			t.Run(ks.Name, func(t *testing.T) {
				for _, shard := range ks.Shards() {
					t.Run(shard.Name, func(t *testing.T) {
						for _, tablet := range shard.Tablets() {
							t.Run(tablet.Alias(), func(t *testing.T) {
								waitForThrottlerStatusEnabled(t, tablet, true, &throttlerConfig{Query: defaultThrottlerQuery, Threshold: customThreshold}, throttlerEnabledTimeout)
							})
						}
					})
				}
			})
		}
	})

	testWithInitialSchema(t)
	t.Run("alter non_online", func(t *testing.T) {
		_ = testOnlineDDLStatement(t, alterTableNormalStatement, string(schema.DDLStrategyDirect), providedUUID, providedMigrationContext, "vtctl", "non_online", "", false)
		insertRows(t, 2)
		testRows(t)
	})
	t.Run("successful online alter, vtgate", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableSuccessfulStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		checkMigrationArtifacts(t, &vtParams, shards, uuid, true)

		rs := readMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			retainArtifactSeconds := row.AsInt64("retain_artifacts_seconds", 0)
			assert.Equal(t, int64(86400), retainArtifactSeconds)

			artifacts := row.AsString("artifacts", "")
			assert.NotContains(t, artifacts, "_vt_HOLD_") // _vt_HOLD table removed at cut-over time
		}

		checkCleanupMigration(t, &vtParams, shards, uuid)

		rs = readMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			retainArtifactSeconds := row.AsInt64("retain_artifacts_seconds", 0)
			assert.Equal(t, int64(-1), retainArtifactSeconds)
			assert.False(t, row["shadow_analyzed_timestamp"].IsNull())
		}
	})
	t.Run("successful online alter, vtctl", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "online", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		checkMigrationArtifacts(t, &vtParams, shards, uuid, true)
	})
	t.Run("successful online alter, vtctl, explicit UUID", func(t *testing.T) {
		insertRows(t, 2)
		providedUUID = "00000000_51c9_11ec_9cf2_0a43f95f28a3"
		providedMigrationContext = "endtoend:0000-1111"
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", false)
		assert.Equal(t, providedUUID, uuid)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		checkMigrationArtifacts(t, &vtParams, shards, uuid, true)
	})
	t.Run("duplicate migration, implicitly ignored", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "online", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", true)
		assert.Equal(t, providedUUID, uuid)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})
	t.Run("fail duplicate migration with different context", func(t *testing.T) {
		_ = testOnlineDDLStatement(t, alterTableTrivialStatement, "online", providedUUID, "endtoend:different-context-0000", "vtctl", "vrepl_col", "rejected", true)
	})
	providedUUID = ""
	providedMigrationContext = ""

	t.Run("successful online alter, postponed, vtgate", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess -postpone-completion", providedUUID, providedMigrationContext, "vtgate", "test_val", "", false)
		// Should be still running!
		_ = waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusRunning)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
		// Issue a complete and wait for successful completion
		checkCompleteMigration(t, &vtParams, shards, uuid, true)
		// This part may take a while, because we depend on vreplicatoin polling
		status := waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)

		testRows(t)
		testMigrationRowCount(t, uuid)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
	})
	// Notes about throttling:
	// In this endtoend test we test both direct tablet API for throttling, as well as VTGate queries.
	// - VTGate queries (`ALTER VITESS_MIGRATION THROTTLE ALL ...`) are sent to all relevant shards/tablets via QueryExecutor
	// - tablet API calls have to be sent per-shard to the primary tablet of that shard
	t.Run("throttled migration", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		insertRows(t, 2)
		throttleAllMigrations(t, &vtParams)
		defer unthrottleAllMigrations(t, &vtParams)

		uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", true)
		_ = waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
		testRows(t)
		checkCancelMigration(t, &vtParams, shards, uuid, true)
		status := waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusCancelled)
	})

	t.Run("throttled and unthrottled migration", func(t *testing.T) {
		insertRows(t, 2)

		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		// begin throttling:
		throttleAllMigrations(t, &vtParams)
		defer unthrottleAllMigrations(t, &vtParams)
		checkThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, true)

		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)
		_ = waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
		testRows(t)

		// gotta give the migration a few seconds to read throttling info from _vt.vreplication and write
		// to _vt.schema_migrations
		row, startedTimestamp, lastThrottledTimestamp := waitForThrottledTimestamp(t, &vtParams, uuid, normalMigrationWait)
		require.NotNil(t, row)
		// vplayer and vcopier update throttle timestamp every second, so we expect the value
		// to be strictly higher than started_timestamp
		assert.GreaterOrEqual(t, lastThrottledTimestamp, startedTimestamp)
		component := row.AsString("component_throttled", "")
		assert.Contains(t, []string{throttlerapp.VCopierName.String(), throttlerapp.VPlayerName.String()}, component)
		reason := row.AsString("reason_throttled", "")
		assert.Contains(t, reason, "is explicitly denied access")

		// unthrottle
		unthrottleAllMigrations(t, &vtParams)
		checkThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, false)

		status := waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})

	t.Run("throttled and unthrottled migration via vstreamer", func(t *testing.T) {
		insertRows(t, 2)
		var uuid string

		func() {
			_, err := throttleAppAndWaitUntilTabletsConfirm(t, throttlerapp.VStreamerName)
			defer unthrottleAppAndWaitUntilTabletsConfirm(t, throttlerapp.VStreamerName)
			require.NoError(t, err)

			uuid = testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)
			_ = waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
			checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
			testRows(t)

			// gotta give the migration a few seconds to read throttling info from _vt.vreplication and write
			// to _vt.schema_migrations
			row, startedTimestamp, lastThrottledTimestamp := waitForThrottledTimestamp(t, &vtParams, uuid, normalMigrationWait)
			require.NotNil(t, row)

			startedTime, err := time.Parse(sqltypes.TimestampFormat, startedTimestamp)
			require.NoError(t, err)
			lastThrottledTime, err := time.Parse(sqltypes.TimestampFormat, lastThrottledTimestamp)
			require.NoError(t, err)

			// rowstreamer throttle timestamp only updates once in 10 seconds, so greater or equals" is good enough here.
			// Technically, lastThrottledTime has to be >= startedTime, but we allow a deviation of 1 sec due to
			// clock irregularities
			assert.GreaterOrEqual(t, lastThrottledTime.Add(time.Second), startedTime)
			component := row.AsString("component_throttled", "")
			assert.Contains(t, []string{throttlerapp.VStreamerName.String(), throttlerapp.RowStreamerName.String()}, component)
		}()
		// now unthrottled
		status := waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})

	t.Run("failed migration", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableFailedStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		testRows(t)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, true)
		checkMigrationArtifacts(t, &vtParams, shards, uuid, true)
		// migration will fail again
	})
	t.Run("failed migration due to vreplication", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableFailedVreplicationStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
		waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)

		rs := readMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			message := row["message"].ToString()
			assert.Contains(t, message, "vreplication: terminal error:", "migration row: %v", row)
		}
	})
	t.Run("cancel all migrations: nothing to cancel", func(t *testing.T) {
		// no migrations pending at this time
		time.Sleep(10 * time.Second)
		checkCancelAllMigrations(t, &vtParams, 0)
		// Validate that invoking CANCEL ALL via vtctl works
		checkCancelAllMigrationsViaVtctld(t, keyspaceName)
	})
	t.Run("cancel all migrations: some migrations to cancel", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		throttleAllMigrations(t, &vtParams)
		defer unthrottleAllMigrations(t, &vtParams)
		checkThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, true)

		// spawn n migrations; cancel them via cancel-all
		var wg sync.WaitGroup
		count := 4
		for range count {
			wg.Go(func() {
				_ = testOnlineDDLStatement(t, alterTableThrottlingStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
			})
		}
		wg.Wait()
		checkCancelAllMigrations(t, &vtParams, len(shards)*count)
	})
	t.Run("cancel all migrations: some migrations to cancel via vtctl", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		throttleAllMigrations(t, &vtParams)
		defer unthrottleAllMigrations(t, &vtParams)
		checkThrottledApps(t, &vtParams, throttlerapp.OnlineDDLName, true)

		// spawn n migrations; cancel them via cancel-all
		var wg sync.WaitGroup
		count := 4
		for range count {
			wg.Go(func() {
				_ = testOnlineDDLStatement(t, alterTableThrottlingStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
			})
		}
		wg.Wait()
		// cancelling via vtctl does not return values. We CANCEL ALL via vtctl, then validate via VTGate that nothing remains to be cancelled.
		checkCancelAllMigrationsViaVtctld(t, keyspaceName)
		checkCancelAllMigrations(t, &vtParams, 0)
	})

	// reparent shard -80 to replica
	// and then reparent it back to original state
	// (two pretty much identical tests, the point is to end up with original state)
	for _, currentPrimaryTabletIndex := range []int{0, 1} {
		currentPrimaryTablet := shardTablets[currentPrimaryTabletIndex]
		reparentTablet := shardTablets[1-currentPrimaryTabletIndex]
		t.Run(fmt.Sprintf("PlannedReparentShard via throttling %d/2", (currentPrimaryTabletIndex+1)), func(t *testing.T) {
			insertRows(t, 2)
			_, err := throttleAppAndWaitUntilTabletsConfirm(t, throttlerapp.OnlineDDLName)
			assert.NoError(t, err)
			defer unthrottleAppAndWaitUntilTabletsConfirm(t, throttlerapp.OnlineDDLName)

			uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)

			t.Run("wait for migration to run", func(t *testing.T) {
				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
			})
			t.Run("wait for vreplication to run on shard -80", func(t *testing.T) {
				vreplStatus := waitForVReplicationStatus(t, currentPrimaryTablet, uuid, normalMigrationWait, binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String())
				require.Contains(t, []string{binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String()}, vreplStatus)
			})
			t.Run("wait for vreplication to run on shard 80-", func(t *testing.T) {
				vreplStatus := waitForVReplicationStatus(t, shards[1].Tablets()[0], uuid, normalMigrationWait, binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String())
				require.Contains(t, []string{binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String()}, vreplStatus)
			})
			t.Run("check status again", func(t *testing.T) {
				// again see that we're still 'running'
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
				testRows(t)
			})

			t.Run("Check tablet", func(t *testing.T) {
				// executor marks this migration with its tablet alias
				// reminder that executor runs on the primary tablet.
				rs := readMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						require.Equal(t, paddedAlias(currentPrimaryTablet), tablet)
					case "80-":
						require.Equal(t, paddedAlias(shards[1].Tablets()[0]), tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}
			})
			t.Run("PRS shard -80", func(t *testing.T) {
				// migration has started and is throttled. We now run PRS
				err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard", keyspaceName+"/-80", "--new-primary", reparentTablet.Alias())
				require.NoError(t, err, "failed PRS: %v", err)
				rs := vtgateExecQuery(t, &vtParams, "show vitess_tablets", "")
				printQueryResult(os.Stdout, rs)
			})
			t.Run("unthrottle", func(t *testing.T) {
				_, err := unthrottleAppAndWaitUntilTabletsConfirm(t, throttlerapp.OnlineDDLName)
				assert.NoError(t, err)
			})
			t.Run("expect completion", func(t *testing.T) {
				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			})

			t.Run("Check tablet post PRS", func(t *testing.T) {
				// executor will find that a vrepl migration started in a different tablet.
				// it will own the tablet and will update 'tablet' column in _vt.schema_migrations with its own
				// (promoted primary) tablet alias.
				rs := readMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						// PRS for this tablet, we promoted tablet[1]
						require.Equal(t, paddedAlias(reparentTablet), tablet)
					case "80-":
						// No PRS for this tablet
						require.Equal(t, paddedAlias(shards[1].Tablets()[0]), tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}

				checkRetryPartialMigration(t, &vtParams, uuid, 1)
				// Now it should complete on the failed shard
				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete)
			})
		})
	}

	// reparent shard -80 to replica
	// and then reparent it back to original state
	// (two pretty much identical tests, the point is to end up with original state)
	for _, currentPrimaryTabletIndex := range []int{0, 1} {
		currentPrimaryTablet := shardTablets[currentPrimaryTabletIndex]
		reparentTablet := shardTablets[1-currentPrimaryTabletIndex]

		t.Run(fmt.Sprintf("PlannedReparentShard via postponed %d/2", (currentPrimaryTabletIndex+1)), func(t *testing.T) {
			insertRows(t, 2)

			uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess --postpone-completion", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)

			t.Run("wait for migration to run", func(t *testing.T) {
				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
			})
			t.Run("wait for vreplication to run on shard -80", func(t *testing.T) {
				vreplStatus := waitForVReplicationStatus(t, currentPrimaryTablet, uuid, normalMigrationWait, binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String())
				require.Contains(t, []string{binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String()}, vreplStatus)
			})
			t.Run("wait for vreplication to run on shard 80-", func(t *testing.T) {
				vreplStatus := waitForVReplicationStatus(t, shards[1].Tablets()[0], uuid, normalMigrationWait, binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String())
				require.Contains(t, []string{binlogdatapb.VReplicationWorkflowState_Copying.String(), binlogdatapb.VReplicationWorkflowState_Running.String()}, vreplStatus)
			})
			t.Run("check status again", func(t *testing.T) {
				// again see that we're still 'running'
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
				testRows(t)
			})

			t.Run("Check tablet", func(t *testing.T) {
				// executor marks this migration with its tablet alias
				// reminder that executor runs on the primary tablet.
				rs := readMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						require.Equal(t, paddedAlias(currentPrimaryTablet), tablet)
					case "80-":
						require.Equal(t, paddedAlias(shards[1].Tablets()[0]), tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}
			})
			t.Run("PRS shard -80", func(t *testing.T) {
				// migration has started and completion is postponed. We now PRS
				err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "PlannedReparentShard", keyspaceName+"/-80", "--new-primary", reparentTablet.Alias())
				require.NoError(t, err, "failed PRS: %v", err)
				rs := vtgateExecQuery(t, &vtParams, "show vitess_tablets", "")
				printQueryResult(os.Stdout, rs)
			})
			t.Run("complete and expect completion", func(t *testing.T) {
				query := fmt.Sprintf("select * from _vt.vreplication where workflow ='%s'", uuid)
				rs, err := reparentTablet.QueryTabletWithDB(t.Context(), query, "")
				assert.NoError(t, err)
				printQueryResult(os.Stdout, rs)

				checkCompleteAllMigrations(t, &vtParams, len(shards))

				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			})

			t.Run("Check tablet post PRS", func(t *testing.T) {
				// executor will find that a vrepl migration started in a different tablet.
				// it will own the tablet and will update 'tablet' column in _vt.schema_migrations with its own
				// (promoted primary) tablet alias.
				rs := readMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						// PRS for this tablet
						require.Equal(t, paddedAlias(reparentTablet), tablet)
					case "80-":
						// No PRS for this tablet
						require.Equal(t, paddedAlias(shards[1].Tablets()[0]), tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}

				checkRetryPartialMigration(t, &vtParams, uuid, 1)
				// Now it should complete on the failed shard
				_ = waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete)
			})
		})
	}

	t.Run("Online DROP, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableStatement, "online", providedUUID, providedMigrationContext, "vtctl", "", "", false)
		t.Run("test ready to complete", func(t *testing.T) {
			rs := readMigrations(t, &vtParams, uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("Online CREATE, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLCreateTableStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "online_ddl_create_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("Online DROP TABLE IF EXISTS, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "online ", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		// this table existed
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 1)
	})
	t.Run("Online CREATE, vtctl, extra flags", func(t *testing.T) {
		// the flags are meaningless to this migration. The test just validates that they don't get in the way.
		uuid := testOnlineDDLStatement(t, onlineDDLCreateTableStatement, "vitess --prefer-instant-ddl --allow-zero-in-date", providedUUID, providedMigrationContext, "vtctl", "online_ddl_create_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("Online DROP TABLE IF EXISTS, vtgate, extra flags", func(t *testing.T) {
		// the flags are meaningless to this migration. The test just validates that they don't get in the way.
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "vitess --prefer-instant-ddl --allow-zero-in-date", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		// this table existed
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 1)
	})
	t.Run("Online DROP TABLE IF EXISTS for nonexistent table, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "online", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		// this table did not exist
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 0)
	})
	t.Run("Online DROP TABLE IF EXISTS for nonexistent table, postponed", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "vitess -postpone-completion", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		// Should be still queued, never promoted to 'ready'!
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued)
		// Issue a complete and wait for successful completion
		checkCompleteMigration(t, &vtParams, shards, uuid, true)
		// This part may take a while, because we depend on vreplicatoin polling
		status := waitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
		// this table did not exist
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 0)
	})
	t.Run("Online DROP TABLE for nonexistent table, expect error, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableStatement, "online", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, true)
	})
	t.Run("Online CREATE, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLCreateTableStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "online_ddl_create_col", "", false)
		checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkCancelMigration(t, &vtParams, shards, uuid, false)
		checkRetryMigration(t, &vtParams, shards, uuid, false)
	})

	// Technically the next test should belong in onlineddl_revert suite. But we're tking advantage of setup and functionality existing in this tets:
	// - two shards as opposed to one
	// - tablet throttling
	t.Run("Revert a migration completed on one shard and cancelled on another", func(t *testing.T) {
		// shard 0 will run normally, shard 1 will be postponed

		var uuid string
		t.Run("run migrations, expect running on both shards", func(t *testing.T) {
			uuid = testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess --postpone-launch", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)
			checkLaunchMigration(t, &vtParams, shards[0:1], uuid, "-80", true)
			{
				status := waitForMigrationStatus(t, &vtParams, shards[:1], uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				checkMigrationStatus(t, &vtParams, shards[:1], uuid, schema.OnlineDDLStatusComplete)
			}
			{
				status := waitForMigrationStatus(t, &vtParams, shards[1:], uuid, normalMigrationWait, schema.OnlineDDLStatusQueued)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				checkMigrationStatus(t, &vtParams, shards[1:], uuid, schema.OnlineDDLStatusQueued)
			}
		})
		t.Run("check cancel migration", func(t *testing.T) {
			checkCancelAllMigrations(t, &vtParams, 1)
		})
		t.Run("launch-all", func(t *testing.T) {
			checkLaunchAllMigrations(t, &vtParams, 0)
		})
		var revertUUID string
		t.Run("issue revert migration", func(t *testing.T) {
			revertQuery := fmt.Sprintf("revert vitess_migration '%s'", uuid)
			rs := vtgateExecQuery(t, &vtParams, revertQuery, "")
			require.NotNil(t, rs)
			row := rs.Named().Row()
			require.NotNil(t, row)
			revertUUID = row.AsString("uuid", "")
			assert.NotEmpty(t, revertUUID)
		})
		t.Run("migrations were cancelled, revert should impossible", func(t *testing.T) {
			{
				// shard 0 migration was complete. Revert should be successful
				status := waitForMigrationStatus(t, &vtParams, shards[:1], revertUUID, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				checkMigrationStatus(t, &vtParams, shards[:1], revertUUID, schema.OnlineDDLStatusFailed)
			}
			{
				// shard 0 migration was cancelled. Revert should not be possible
				status := waitForMigrationStatus(t, &vtParams, shards[1:], revertUUID, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				checkMigrationStatus(t, &vtParams, shards[1:], revertUUID, schema.OnlineDDLStatusFailed)
			}
		})
		t.Run("expect two rows in SHOW VITESS_MIGRATIONS", func(t *testing.T) {
			// This validates that the shards are reflected correctly in output of SHOW VITESS_MIGRATIONS
			rs := readMigrations(t, &vtParams, revertUUID)
			require.NotNil(t, rs)
			require.Equal(t, 2, len(rs.Rows))
			for _, row := range rs.Named().Rows {
				shard := row["shard"].ToString()
				status := row["migration_status"].ToString()

				switch shard {
				case "-80":
					require.Equal(t, string(schema.OnlineDDLStatusComplete), status)
				case "80-":
					require.Equal(t, string(schema.OnlineDDLStatusFailed), status)
				default:
					require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
				}
			}
		})
	})
	t.Run("Revert a migration completed on both shards", func(t *testing.T) {
		var uuid string
		t.Run("run migration, expect completion on both shards", func(t *testing.T) {
			uuid = testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", false)
			checkMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		})
		var revertUUID string
		t.Run("issue revert migration", func(t *testing.T) {
			revertQuery := fmt.Sprintf("revert vitess_migration '%s'", uuid)
			output, err := applySchemaWithOutput(t.Context(), keyspaceName, revertQuery, applySchemaParams{DDLStrategy: "vitess"})
			require.NoError(t, err)
			revertUUID = strings.TrimSpace(output)
			assert.NotEmpty(t, revertUUID)
		})
		t.Run("revert completes on both shards", func(t *testing.T) {
			status := waitForMigrationStatus(t, &vtParams, shards, revertUUID, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			checkMigrationStatus(t, &vtParams, shards, revertUUID, schema.OnlineDDLStatusComplete)
		})
		t.Run("validate both shards show complete in SHOW VITESS_MIGRATIONS", func(t *testing.T) {
			rs := readMigrations(t, &vtParams, revertUUID)
			require.NotNil(t, rs)
			require.Equal(t, 2, len(rs.Rows))
			for _, row := range rs.Named().Rows {
				status := row["migration_status"].ToString()
				assert.Equal(t, string(schema.OnlineDDLStatusComplete), status, "shard %s", row["shard"].ToString())
			}
		})
	})
	t.Run("summary: validate sequential migration IDs", func(t *testing.T) {
		validateSequentialMigrationIDs(t, &vtParams, shards)
	})
	t.Run("summary: validate completed_timestamp", func(t *testing.T) {
		validateCompletedTimestamp(t, &vtParams)
	})
}

func insertRow(t *testing.T) {
	insertMutex.Lock()
	defer insertMutex.Unlock()

	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(insertRowStatement, tableName, countInserts)
	r := vtgateExecQuery(t, &vtParams, sqlQuery, "")
	require.NotNil(t, r)
	countInserts++
}

func insertRows(t *testing.T, count int) {
	for range count {
		insertRow(t)
	}
}

func testRows(t *testing.T) {
	insertMutex.Lock()
	defer insertMutex.Unlock()

	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(selectCountRowsStatement, tableName)
	r := vtgateExecQuery(t, &vtParams, sqlQuery, "")
	require.NotNil(t, r)
	row := r.Named().Row()
	require.NotNil(t, row)
	require.Equal(t, countInserts, row.AsInt64("c", 0))
}

func testMigrationRowCount(t *testing.T, uuid string) {
	insertMutex.Lock()
	defer insertMutex.Unlock()

	var totalRowsCopied uint64
	// count sum of rows copied in all shards, that should be the total number of rows inserted to the table
	rs := readMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	for _, row := range rs.Named().Rows {
		rowsCopied := row.AsUint64("rows_copied", 0)
		totalRowsCopied += rowsCopied
	}
	require.Equal(t, uint64(countInserts), totalRowsCopied)
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	sqlQuery := ""
	for i := range totalTableCount {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_onlineddl_test_%02d", i))
		err := applySchema(t.Context(), keyspaceName, sqlQuery)
		require.Nil(t, err)
	}

	// Check if 4 tables are created
	checkTables(t, "", totalTableCount)
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, providedUUIDList string, providedMigrationContext string, executeStrategy string, expectHint string, expectError string, skipWait bool) (uuid string) {
	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(alterStatement, tableName)
	if executeStrategy == "vtgate" {
		row := vtgateExecDDL(t, &vtParams, ddlStrategy, sqlQuery, "").Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	} else {
		params := applySchemaParams{DDLStrategy: ddlStrategy, UUIDs: providedUUIDList, MigrationContext: providedMigrationContext}
		output, err := applySchemaWithOutput(t.Context(), keyspaceName, sqlQuery, params)
		if expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, expectError)
		}
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	if strategySetting.Strategy.IsDirect() {
		skipWait = true
	}
	if !skipWait {
		status := waitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectError == "" && expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, showTableName string, expectCount int) {
	for _, shard := range shards {
		checkTablesCount(t, shard.Tablets()[0], showTableName, expectCount)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *vitesst.Tablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.QueryTablet(t.Context(), query)
	require.Nil(t, err)
	assert.Equal(t, expectCount, len(queryResult.Rows))
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectColumn string) {
	for _, shard := range shards {
		createStatement := getCreateTableStatement(t, shard.Tablets()[0], tableName)
		assert.Contains(t, createStatement, expectColumn)
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
