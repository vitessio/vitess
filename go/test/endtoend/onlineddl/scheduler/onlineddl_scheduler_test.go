/*
Copyright 2021 The Vitess Authors.

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

package scheduler

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	ddlStrategy           = "online"
	t1Name                = "t1_test"
	t2Name                = "t2_test"
	createT1Statement     = `
		CREATE TABLE t1_test (
			id bigint(20) not null,
			hint_col varchar(64) not null default 'just-created',
			PRIMARY KEY (id)
		) ENGINE=InnoDB
	`
	createT2Statement = `
		CREATE TABLE t2_test (
			id bigint(20) not null,
			hint_col varchar(64) not null default 'just-created',
			PRIMARY KEY (id)
		) ENGINE=InnoDB
	`
	trivialAlterT1Statement = `
		ALTER TABLE t1_test ENGINE=InnoDB;
	`
	trivialAlterT2Statement = `
		ALTER TABLE t2_test ENGINE=InnoDB;
	`
	dropT1Statement = `
		DROP TABLE IF EXISTS t1_test
	`
	dropT3Statement = `
		DROP TABLE IF EXISTS t3_test
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
			"-schema_change_dir", schemaChangeDirectory,
			"-schema_change_controller", "local",
			"-schema_change_check_interval", "1"}

		clusterInstance.VtTabletExtraArgs = []string{
			"-enable-lag-throttler",
			"-throttle_threshold", "1s",
			"-heartbeat_enable",
			"-heartbeat_interval", "250ms",
		}
		clusterInstance.VtGateExtraArgs = []string{}

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
		// set the gateway we want to use
		vtgateInstance.GatewayImplementation = "tabletgateway"
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
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	var t1uuid string
	var t2uuid string

	testCompareTableTimes := func(t *testing.T, t1uuid, t2uuid string) {
		t.Run("Compare t1, t2 times", func(t *testing.T) {
			var t1endTime, t2startTime string
			{
				rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					t1endTime = row.AsString("completed_timestamp", "")
					assert.NotEmpty(t, t1endTime)
				}
			}
			{
				rs := onlineddl.ReadMigrations(t, &vtParams, t2uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					t2startTime = row.AsString("started_timestamp", "")
					assert.NotEmpty(t, t2startTime)
				}
			}
			assert.GreaterOrEqual(t, t2startTime, t1endTime)
		})
	}

	// CREATE
	t.Run("CREATE TABLEs t1, t1", func(t *testing.T) {
		{ // The table does not exist
			t1uuid = testOnlineDDLStatement(t, createT1Statement, ddlStrategy, "vtgate", "just-created", "", false)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		}
		{
			// The table does not exist
			t2uuid = testOnlineDDLStatement(t, createT2Statement, ddlStrategy, "vtgate", "just-created", "", false)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t2Name, true)
		}
		testCompareTableTimes(t, t1uuid, t2uuid)
	})
	t.Run("ALTER both tables non-concurrent", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, trivialAlterT1Statement, ddlStrategy, "vtgate", "", "", true) // skip wait
		t2uuid = testOnlineDDLStatement(t, trivialAlterT2Statement, ddlStrategy, "vtgate", "", "", true) // skip wait

		{
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		}
		{
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		}
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)

		testCompareTableTimes(t, t1uuid, t2uuid)
	})
	t.Run("ALTER both tables non-concurrent, postponed", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, trivialAlterT1Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true) // skip wait
		t2uuid = testOnlineDDLStatement(t, trivialAlterT2Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true) // skip wait

		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusRunning)
		// now that t1 is running, let's unblock t2. We expect it to remain queued.
		onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
		time.Sleep(5 * time.Second)
		// t1 should be still running!
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		// non-concurrent -- should be queued!
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		{
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplicatoin polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		}
		{
			// This part may take a while, because we depend on vreplicatoin polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		}
		testCompareTableTimes(t, t1uuid, t2uuid)
	})
	t.Run("REVERT both tables concurrent, postponed", func(t *testing.T) {
		t1uuid = testRevertMigration(t, t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true)
		t2uuid = testRevertMigration(t, t2uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true)

		t.Run("expect both migrations to run", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusRunning)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, 20*time.Second, schema.OnlineDDLStatusRunning)
		})
		{
			// now that both are running, let's unblock t2. We expect it to complete.
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplicatoin polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		}
		{
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplicatoin polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		}
	})
	t.Run("one concurrent REVERT, one conflicting DROP, one non-conflicting DROP", func(t *testing.T) {
		t1uuid = testRevertMigration(t, t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true)
		drop3uuid := testOnlineDDLStatement(t, dropT3Statement, ddlStrategy, "vtgate", "", "", true) // skip wait

		t.Run("expect t1 migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, dropT1Statement, ddlStrategy, "vtgate", "", "", true) // skip wait
		{
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop3uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		}
		{
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, drop1uuid, true)
			time.Sleep(2 * time.Second)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusCancelled)
		}
		{
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplicatoin polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		}
	})

	// t.Run("revert CREATE TABLE", func(t *testing.T) {
	// 	// The table existed, so it will now be dropped (renamed)
	// 	uuid := testRevertMigration(t, uuids[len(uuids)-1], "vtgate", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	checkTable(t, tableName, false)
	// })
	// t.Run("revert revert CREATE TABLE", func(t *testing.T) {
	// 	// Table was dropped (renamed) so it will now be restored
	// 	uuid := testRevertMigration(t, uuids[len(uuids)-1], "vtgate", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	checkTable(t, tableName, true)
	// })

	// var throttledUUID string
	// t.Run("throttled migration", func(t *testing.T) {
	// 	throttledUUID = testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost -singleton --max-load=Threads_running=1", "vtgate", "hint_col", "", false)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusRunning)
	// })
	// t.Run("failed singleton migration, vtgate", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost -singleton --max-load=Threads_running=1", "vtgate", "hint_col", "rejected", true)
	// 	assert.Empty(t, uuid)
	// })
	// t.Run("failed singleton migration, vtctl", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost -singleton --max-load=Threads_running=1", "vtctl", "hint_col", "rejected", true)
	// 	assert.Empty(t, uuid)
	// })
	// t.Run("failed revert migration", func(t *testing.T) {
	// 	uuid := testRevertMigration(t, throttledUUID, "vtgate", "rejected", true)
	// 	assert.Empty(t, uuid)
	// })
	// t.Run("terminate throttled migration", func(t *testing.T) {
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusRunning)
	// 	onlineddl.CheckCancelMigration(t, &vtParams, shards, throttledUUID, true)
	// 	time.Sleep(2 * time.Second)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusFailed)
	// })
	// t.Run("successful gh-ost alter, vtctl", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost -singleton", "vtctl", "hint_col", "", false)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
	// 	onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	// })
	// t.Run("successful gh-ost alter, vtgate", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost -singleton", "vtgate", "hint_col", "", false)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
	// 	onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	// })

	// t.Run("successful online alter, vtgate", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, onlineSingletonDDLStrategy, "vtgate", "hint_col", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
	// 	onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	// 	checkTable(t, tableName, true)
	// })
	// t.Run("revert ALTER TABLE, vttablet", func(t *testing.T) {
	// 	// The table existed, so it will now be dropped (renamed)
	// 	uuid := testRevertMigration(t, uuids[len(uuids)-1], "vttablet", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	checkTable(t, tableName, true)
	// })

	// var throttledUUIDs []string
	// // singleton-context
	// t.Run("throttled migrations, singleton-context", func(t *testing.T) {
	// 	uuidList := testOnlineDDLStatement(t, multiAlterTableThrottlingStatement, "gh-ost -singleton-context --max-load=Threads_running=1", "vtctl", "hint_col", "", false)
	// 	throttledUUIDs = strings.Split(uuidList, "\n")
	// 	assert.Equal(t, 3, len(throttledUUIDs))
	// 	for _, uuid := range throttledUUIDs {
	// 		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning, schema.OnlineDDLStatusQueued)
	// 	}
	// })
	// t.Run("failed migrations, singleton-context", func(t *testing.T) {
	// 	_ = testOnlineDDLStatement(t, multiAlterTableThrottlingStatement, "gh-ost -singleton-context --max-load=Threads_running=1", "vtctl", "hint_col", "rejected", false)
	// })
	// t.Run("terminate throttled migrations", func(t *testing.T) {
	// 	for _, uuid := range throttledUUIDs {
	// 		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusRunning, schema.OnlineDDLStatusQueued)
	// 		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, true)
	// 	}
	// 	time.Sleep(2 * time.Second)
	// 	for _, uuid := range throttledUUIDs {
	// 		uuid = strings.TrimSpace(uuid)
	// 		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
	// 	}
	// })

	// t.Run("successful multiple statement, singleton-context, vtctl", func(t *testing.T) {
	// 	uuidList := testOnlineDDLStatement(t, multiDropStatements, onlineSingletonContextDDLStrategy, "vtctl", "", "", false)
	// 	uuidSlice := strings.Split(uuidList, "\n")
	// 	assert.Equal(t, 3, len(uuidSlice))
	// 	for _, uuid := range uuidSlice {
	// 		uuid = strings.TrimSpace(uuid)
	// 		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	}
	// })

	// //DROP

	// t.Run("online DROP TABLE", func(t *testing.T) {
	// 	uuid := testOnlineDDLStatement(t, dropStatement, onlineSingletonDDLStrategy, "vtgate", "", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	checkTable(t, tableName, false)
	// })
	// t.Run("revert DROP TABLE", func(t *testing.T) {
	// 	// This will recreate the table (well, actually, rename it back into place)
	// 	uuid := testRevertMigration(t, uuids[len(uuids)-1], "vttablet", "", false)
	// 	uuids = append(uuids, uuid)
	// 	onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	// 	checkTable(t, tableName, true)
	// })

	// // Last two tests (we run an incomplete migration)
	// t.Run("submit successful migration, no wait, vtgate", func(t *testing.T) {
	// 	_ = testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost -singleton", "vtgate", "hint_col", "", true)
	// })
	// t.Run("fail submit migration, no wait, vtgate", func(t *testing.T) {
	// 	_ = testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost -singleton", "vtgate", "hint_col", "rejected", true)
	// })
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, ddlStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) (uuid string) {
	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	require.NoError(t, err)

	stmt, err := sqlparser.Parse(ddlStatement)
	require.NoError(t, err)
	ddlStmt, ok := stmt.(sqlparser.DDLStatement)
	require.True(t, ok)
	tableName := ddlStmt.GetTable().Name.String()

	if executeStrategy == "vtgate" {
		result := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, ddlStatement, expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, ddlStatement, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true})
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

	if !strategySetting.Strategy.IsDirect() && !skipWait {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectError == "" && expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, revertUUID string, ddlStrategy, executeStrategy string, expectError string, skipWait bool) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", revertUUID)
	if executeStrategy == "vtgate" {
		result := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, revertQuery, expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, revertQuery, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true})
		if expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, expectError)
		}
	}

	if expectError == "" {
		uuid = strings.TrimSpace(uuid)
		fmt.Println("# Generated UUID (for debug purposes):")
		fmt.Printf("<%s>\n", uuid)
	}
	if !skipWait {
		time.Sleep(time.Second * 20)
	}
	return uuid
}

// checkTable checks the number of tables in the first two shards.
func checkTable(t *testing.T, showTableName string, expectExists bool) bool {
	expectCount := 0
	if expectExists {
		expectCount = 1
	}
	for i := range clusterInstance.Keyspaces[0].Shards {
		if !checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[i].Vttablets[0], showTableName, expectCount) {
			return false
		}
	}
	return true
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) bool {
	query := fmt.Sprintf(`show tables like '%%%s%%';`, showTableName)
	queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.Nil(t, err)
	return assert.Equal(t, expectCount, len(queryResult.Rows))
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
