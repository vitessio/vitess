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
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vt/schema"
	throttlebase "vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance  *cluster.LocalProcessCluster
	shards           []cluster.Shard
	vtParams         mysql.ConnParams
	httpClient       = throttlebase.SetupHTTPClient(time.Second)
	throttlerAppName = "online-ddl"

	normalMigrationWait   = 20 * time.Second
	extendedMigrationWait = 20 * time.Second

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
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
			"--schema_change_check_interval", "1",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "5s",
		}
		clusterInstance.VtGateExtraArgs = []string{
			"--ddl_strategy", "online",
		}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		keyspace := &cluster.Keyspace{
			Name:    keyspaceName,
			VSchema: vSchema,
		}

		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
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

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

// direct per-tablet throttler API instruction
func throttleResponse(tablet *cluster.Vttablet, path string) (resp *http.Response, respBody string, err error) {
	apiURL := fmt.Sprintf("http://%s:%d/%s", tablet.VttabletProcess.TabletHostname, tablet.HTTPPort, path)
	resp, err = httpClient.Get(apiURL)
	if err != nil {
		return resp, respBody, err
	}
	b, err := io.ReadAll(resp.Body)
	respBody = string(b)
	return resp, respBody, err
}

// direct per-tablet throttler API instruction
func throttleApp(tablet *cluster.Vttablet, app string) (*http.Response, string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/throttle-app?app=%s&duration=1h", app))
}

// direct per-tablet throttler API instruction
func unthrottleApp(tablet *cluster.Vttablet, app string) (*http.Response, string, error) {
	return throttleResponse(tablet, fmt.Sprintf("throttler/unthrottle-app?app=%s", app))
}

func TestSchemaChange(t *testing.T) {
	defer cluster.PanicHandler(t)

	shards = clusterInstance.Keyspaces[0].Shards
	assert.Equal(t, 2, len(shards))
	for _, shard := range shards {
		assert.Equal(t, 2, len(shard.Vttablets))
	}

	providedUUID := ""
	providedMigrationContext := ""
	testWithInitialSchema(t)
	t.Run("alter non_online", func(t *testing.T) {
		_ = testOnlineDDLStatement(t, alterTableNormalStatement, string(schema.DDLStrategyDirect), providedUUID, providedMigrationContext, "vtctl", "non_online", "", false)
		insertRows(t, 2)
		testRows(t)
	})
	t.Run("successful online alter, vtgate", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableSuccessfulStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)

		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			retainArtifactSeconds := row.AsInt64("retain_artifacts_seconds", 0)
			assert.Equal(t, int64(86400), retainArtifactSeconds)
		}

		onlineddl.CheckCleanupMigration(t, &vtParams, shards, uuid)

		rs = onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			retainArtifactSeconds := row.AsInt64("retain_artifacts_seconds", 0)
			assert.Equal(t, int64(-1), retainArtifactSeconds)
		}
	})
	t.Run("successful online alter, vtctl", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "online", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
	})
	t.Run("successful online alter, vtctl, explicit UUID", func(t *testing.T) {
		insertRows(t, 2)
		providedUUID = "00000000_51c9_11ec_9cf2_0a43f95f28a3"
		providedMigrationContext = "endtoend:0000-1111"
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", false)
		assert.Equal(t, providedUUID, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		testRows(t)
		testMigrationRowCount(t, uuid)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
	})
	t.Run("duplicate migration, implicitly ignored", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "online", providedUUID, providedMigrationContext, "vtctl", "vrepl_col", "", true)
		assert.Equal(t, providedUUID, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
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
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
		// Issue a complete and wait for successful completion
		onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
		// This part may take a while, because we depend on vreplicatoin polling
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)

		testRows(t)
		testMigrationRowCount(t, uuid)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
	// Notes about throttling:
	// In this endtoend test we test both direct tablet API for throttling, as well as VTGate queries.
	// - VTGate queries (`ALTER VITESS_MIGRATION THROTTLE ALL ...`) are sent to all relevant shards/tablets via QueryExecutor
	// - tablet API calls have to be sent per-shard to the primary tablet of that shard
	t.Run("throttled migration", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		insertRows(t, 2)
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)

		uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", true)
		_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
		testRows(t)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, true)
		time.Sleep(2 * time.Second)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
	})

	t.Run("throttled and unthrottled migration", func(t *testing.T) {
		insertRows(t, 2)

		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		// begin throttling:
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)
		onlineddl.CheckThrottledApps(t, &vtParams, throttlerAppName, true)

		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)
		_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
		testRows(t)

		// unthrottle
		onlineddl.UnthrottleAllMigrations(t, &vtParams)
		onlineddl.CheckThrottledApps(t, &vtParams, throttlerAppName, false)

		_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})

	t.Run("failed migration", func(t *testing.T) {
		insertRows(t, 2)
		uuid := testOnlineDDLStatement(t, alterTableFailedStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		testRows(t)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, true)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		// migration will fail again
	})
	t.Run("cancel all migrations: nothing to cancel", func(t *testing.T) {
		// no migrations pending at this time
		time.Sleep(10 * time.Second)
		onlineddl.CheckCancelAllMigrations(t, &vtParams, 0)
		// Validate that invoking CANCEL ALL via vtctl works
		onlineddl.CheckCancelAllMigrationsViaVtctl(t, &clusterInstance.VtctlclientProcess, keyspaceName)
	})
	t.Run("cancel all migrations: some migrations to cancel", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)
		onlineddl.CheckThrottledApps(t, &vtParams, throttlerAppName, true)

		// spawn n migrations; cancel them via cancel-all
		var wg sync.WaitGroup
		count := 4
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = testOnlineDDLStatement(t, alterTableThrottlingStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
			}()
		}
		wg.Wait()
		onlineddl.CheckCancelAllMigrations(t, &vtParams, len(shards)*count)
	})
	t.Run("cancel all migrations: some migrations to cancel via vtctl", func(t *testing.T) {
		// Use VTGate for throttling, issue a `ALTER VITESS_MIGRATION THROTTLE ALL ...`
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)
		onlineddl.CheckThrottledApps(t, &vtParams, throttlerAppName, true)

		// spawn n migrations; cancel them via cancel-all
		var wg sync.WaitGroup
		count := 4
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = testOnlineDDLStatement(t, alterTableThrottlingStatement, "online", providedUUID, providedMigrationContext, "vtgate", "vrepl_col", "", false)
			}()
		}
		wg.Wait()
		// cancelling via vtctl does not return values. We CANCEL ALL via vtctl, then validate via VTGate that nothing remains to be cancelled.
		onlineddl.CheckCancelAllMigrationsViaVtctl(t, &clusterInstance.VtctlclientProcess, keyspaceName)
		onlineddl.CheckCancelAllMigrations(t, &vtParams, 0)
	})

	// reparent shard -80 to replica
	// and then reparent it back to original state
	// (two pretty much identical tests, the point is to end up with original state)
	for currentPrimaryTabletIndex, reparentTabletIndex := range []int{1, 0} {
		t.Run(fmt.Sprintf("PlannedReparentShard via throttling %d/2", (currentPrimaryTabletIndex+1)), func(t *testing.T) {

			insertRows(t, 2)
			for i := range shards {
				var body string
				var err error
				switch i {
				case 0:
					// this is the shard where we run PRS
					// Use per-tablet throttling API
					_, body, err = throttleApp(shards[i].Vttablets[currentPrimaryTabletIndex], throttlerAppName)
					defer unthrottleApp(shards[i].Vttablets[currentPrimaryTabletIndex], throttlerAppName)
				case 1:
					// no PRS on this shard
					// Use per-tablet throttling API
					_, body, err = throttleApp(shards[i].Vttablets[0], throttlerAppName)
					defer unthrottleApp(shards[i].Vttablets[0], throttlerAppName)
				}
				assert.NoError(t, err)
				assert.Contains(t, body, throttlerAppName)
			}
			uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess", providedUUID, providedMigrationContext, "vtgate", "test_val", "", true)

			t.Run("wait for migration and vreplication to run", func(t *testing.T) {
				_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusRunning)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
				time.Sleep(5 * time.Second) // wait for _vt.vreplication to be created
				vreplStatus := onlineddl.WaitForVReplicationStatus(t, &vtParams, shards, uuid, normalMigrationWait, "Copying")
				require.Equal(t, "Copying", vreplStatus)
				// again see that we're still 'running'
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning)
				testRows(t)
			})

			t.Run("Check tablet", func(t *testing.T) {
				// onlineddl.Executor marks this migration with its tablet alias
				// reminder that onlineddl.Executor runs on the primary tablet.
				rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						require.Equal(t, shards[0].Vttablets[currentPrimaryTabletIndex].Alias, tablet)
					case "80-":
						require.Equal(t, shards[1].Vttablets[0].Alias, tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}
			})
			t.Run("PRS shard -80", func(t *testing.T) {
				// migration has started and is throttled. We now run PRS
				err := clusterInstance.VtctlclientProcess.ExecuteCommand("PlannedReparentShard", "--", "--keyspace_shard", keyspaceName+"/-80", "--new_primary", shards[0].Vttablets[reparentTabletIndex].Alias)
				require.NoError(t, err, "failed PRS: %v", err)
			})

			t.Run("unthrottle and expect completion", func(t *testing.T) {
				for i := range shards {
					var body string
					var err error
					switch i {
					case 0:
						// this is the shard where we run PRS
						// Use per-tablet throttling API
						_, body, err = unthrottleApp(shards[i].Vttablets[currentPrimaryTabletIndex], throttlerAppName)
					case 1:
						// no PRS on this shard
						// Use per-tablet throttling API
						_, body, err = unthrottleApp(shards[i].Vttablets[0], throttlerAppName)
					}
					assert.NoError(t, err)
					assert.Contains(t, body, throttlerAppName)
				}

				_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			})

			t.Run("Check tablet post PRS", func(t *testing.T) {
				// onlineddl.Executor will find that a vrepl migration started in a different tablet.
				// it will own the tablet and will update 'tablet' column in _vt.schema_migrations with its own
				// (promoted primary) tablet alias.
				rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					shard := row["shard"].ToString()
					tablet := row["tablet"].ToString()

					switch shard {
					case "-80":
						// PRS for this tablet, we promoted tablet[1]
						require.Equal(t, shards[0].Vttablets[reparentTabletIndex].Alias, tablet)
					case "80-":
						// No PRS for this tablet
						require.Equal(t, shards[1].Vttablets[0].Alias, tablet)
					default:
						require.NoError(t, fmt.Errorf("unexpected shard name: %s", shard))
					}
				}
			})
		})
	}
	t.Run("Online DROP, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableStatement, "online", providedUUID, providedMigrationContext, "vtctl", "", "", false)
		t.Run("test ready to complete", func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("Online CREATE, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLCreateTableStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "online_ddl_create_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("Online DROP TABLE IF EXISTS, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "online", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		// this table existed
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 1)
	})
	t.Run("Online DROP TABLE IF EXISTS for nonexistent table, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "online", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		// this table did not exist
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 0)
	})
	t.Run("Online DROP TABLE IF EXISTS for nonexistent table, postponed", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableIfExistsStatement, "vitess -postpone-completion", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		// Should be still queued, never promoted to 'ready'!
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued)
		// Issue a complete and wait for successful completion
		onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
		// This part may take a while, because we depend on vreplicatoin polling
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, extendedMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		// this table did not exist
		checkTables(t, schema.OnlineDDLToGCUUID(uuid), 0)
	})
	t.Run("Online DROP TABLE for nonexistent table, expect error, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLDropTableStatement, "online", providedUUID, providedMigrationContext, "vtgate", "", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, true)
	})
	t.Run("Online CREATE, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, onlineDDLCreateTableStatement, "vitess", providedUUID, providedMigrationContext, "vtctl", "online_ddl_create_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
}

func insertRow(t *testing.T) {
	insertMutex.Lock()
	defer insertMutex.Unlock()

	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(insertRowStatement, tableName, countInserts)
	r := onlineddl.VtgateExecQuery(t, &vtParams, sqlQuery, "")
	require.NotNil(t, r)
	countInserts++
}

func insertRows(t *testing.T, count int) {
	for i := 0; i < count; i++ {
		insertRow(t)
	}
}

func testRows(t *testing.T) {
	insertMutex.Lock()
	defer insertMutex.Unlock()

	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(selectCountRowsStatement, tableName)
	r := onlineddl.VtgateExecQuery(t, &vtParams, sqlQuery, "")
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
	rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
	require.NotNil(t, rs)
	for _, row := range rs.Named().Rows {
		rowsCopied := row.AsUint64("rows_copied", 0)
		totalRowsCopied += rowsCopied
	}
	require.Equal(t, uint64(countInserts), totalRowsCopied)
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	var sqlQuery = "" //nolint
	for i := 0; i < totalTableCount; i++ {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_onlineddl_test_%02d", i))
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
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
		row := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, sqlQuery, "").Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	} else {
		params := cluster.VtctlClientParams{DDLStrategy: ddlStrategy, UUIDList: providedUUIDList, MigrationContext: providedMigrationContext}
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, sqlQuery, params)
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
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalMigrationWait, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectError == "" && expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, showTableName string, expectCount int) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, expectCount)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, expectCount, len(queryResult.Rows))
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName, expectColumn string) {
	for i := range clusterInstance.Keyspaces[0].Shards {
		createStatement := getCreateTableStatement(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], tableName)
		assert.Contains(t, createStatement, expectColumn)
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
