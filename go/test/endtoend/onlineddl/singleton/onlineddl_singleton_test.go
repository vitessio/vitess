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

package singleton

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

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	hostname                          = "localhost"
	keyspaceName                      = "ks"
	cell                              = "zone1"
	schemaChangeDirectory             = ""
	tableName                         = `stress_test`
	onlineSingletonDDLStrategy        = "online --singleton"
	onlineSingletonContextDDLStrategy = "online --singleton-context"
	createStatement                   = `
		CREATE TABLE stress_test (
			id bigint(20) not null,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default 'just-created',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
	`
	// We will run this query with "gh-ost --max-load=Threads_running=1"
	alterTableThrottlingStatement = `
		ALTER TABLE stress_test DROP COLUMN created_timestamp
	`
	multiAlterTableThrottlingStatement = `
		ALTER TABLE stress_test ENGINE=InnoDB;
		ALTER TABLE stress_test ENGINE=InnoDB;
		ALTER TABLE stress_test ENGINE=InnoDB;
	`
	// A trivial statement which must succeed and does not change the schema
	alterTableTrivialStatement = `
		ALTER TABLE stress_test ENGINE=InnoDB
	`
	dropStatement = `
		DROP TABLE stress_test
	`
	dropNonexistentTableStatement = `
		DROP TABLE IF EXISTS t_non_existent
	`
	multiDropStatements = `DROP TABLE IF EXISTS t1; DROP TABLE IF EXISTS t2; DROP TABLE IF EXISTS t3;`
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
			"--schema_change_check_interval", "1"}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--watch_replication_stream",
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

	var uuids []string
	// CREATE
	t.Run("CREATE TABLE", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatement(t, createStatement, onlineSingletonDDLStrategy, "vtgate", "", "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})
	t.Run("revert CREATE TABLE", func(t *testing.T) {
		// The table existed, so it will now be dropped (renamed)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], "vtgate", onlineSingletonDDLStrategy, "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert revert CREATE TABLE", func(t *testing.T) {
		// Table was dropped (renamed) so it will now be restored
		uuid := testRevertMigration(t, uuids[len(uuids)-1], "vtgate", onlineSingletonDDLStrategy, "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	var throttledUUID string
	t.Run("throttled migration", func(t *testing.T) {
		throttledUUID = testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtgate", "", "hint_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusRunning)
	})
	t.Run("failed singleton migration, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtgate", "", "hint_col", "rejected", true)
		assert.Empty(t, uuid)
	})
	t.Run("failed singleton migration, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtctl", "", "hint_col", "rejected", true)
		assert.Empty(t, uuid)
	})
	t.Run("failed revert migration", func(t *testing.T) {
		uuid := testRevertMigration(t, throttledUUID, "vtgate", onlineSingletonDDLStrategy, "", "rejected", true)
		assert.Empty(t, uuid)
	})
	t.Run("terminate throttled migration", func(t *testing.T) {
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusRunning)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, throttledUUID, true)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, throttledUUID, 20*time.Second, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusCancelled)
	})
	t.Run("successful gh-ost alter, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost --singleton", "vtctl", "", "hint_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("successful gh-ost alter, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "gh-ost --singleton", "vtgate", "", "hint_col", "", false)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})

	t.Run("successful online alter, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, onlineSingletonDDLStrategy, "vtgate", "", "hint_col", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		checkTable(t, tableName, true)
	})
	t.Run("revert ALTER TABLE, vttablet", func(t *testing.T) {
		// The table existed, so it will now be dropped (renamed)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], "vtctl", onlineSingletonDDLStrategy, "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	var throttledUUIDs []string
	// singleton-context
	t.Run("throttled migrations, singleton-context", func(t *testing.T) {
		uuidList := testOnlineDDLStatement(t, multiAlterTableThrottlingStatement, "gh-ost --singleton-context --max-load=Threads_running=1", "vtctl", "", "hint_col", "", false)
		throttledUUIDs = strings.Split(uuidList, "\n")
		assert.Equal(t, 3, len(throttledUUIDs))
		for _, uuid := range throttledUUIDs {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady, schema.OnlineDDLStatusRunning)
		}
	})
	t.Run("failed migrations, singleton-context", func(t *testing.T) {
		_ = testOnlineDDLStatement(t, multiAlterTableThrottlingStatement, "gh-ost --singleton-context --max-load=Threads_running=1", "vtctl", "", "hint_col", "rejected", false)
	})
	t.Run("terminate throttled migrations", func(t *testing.T) {
		for _, uuid := range throttledUUIDs {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady, schema.OnlineDDLStatusRunning)
			onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, true)
		}
		time.Sleep(2 * time.Second)
		for _, uuid := range throttledUUIDs {
			uuid = strings.TrimSpace(uuid)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		}
	})

	t.Run("successful multiple statement, singleton-context, vtctl", func(t *testing.T) {
		uuidList := testOnlineDDLStatement(t, multiDropStatements, onlineSingletonContextDDLStrategy, "vtctl", "", "", "", false)
		uuidSlice := strings.Split(uuidList, "\n")
		assert.Equal(t, 3, len(uuidSlice))
		for _, uuid := range uuidSlice {
			uuid = strings.TrimSpace(uuid)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		}
	})

	//DROP

	t.Run("online DROP TABLE", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, dropStatement, onlineSingletonDDLStrategy, "vtgate", "", "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE", func(t *testing.T) {
		// This will recreate the table (well, actually, rename it back into place)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], "vttablet", onlineSingletonDDLStrategy, "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	t.Run("fail concurrent singleton, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, alterTableTrivialStatement, "vitess --postpone-completion --singleton", "vtgate", "", "hint_col", "", true)
		uuids = append(uuids, uuid)
		_ = testOnlineDDLStatement(t, dropNonexistentTableStatement, "vitess --singleton", "vtgate", "", "hint_col", "rejected", true)
		onlineddl.CheckCompleteAllMigrations(t, &vtParams, len(shards))
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})
	t.Run("fail concurrent singleton-context with revert", func(t *testing.T) {
		revertUUID := testRevertMigration(t, uuids[len(uuids)-1], "vtctl", "vitess --allow-concurrent --postpone-completion --singleton-context", "rev:ctx", "", false)
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusRunning)
		// revert is running
		_ = testOnlineDDLStatement(t, dropNonexistentTableStatement, "vitess --allow-concurrent --singleton-context", "vtctl", "migrate:ctx", "", "rejected", true)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, revertUUID, true)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertUUID, schema.OnlineDDLStatusCancelled)
	})
	t.Run("success concurrent singleton-context with no-context revert", func(t *testing.T) {
		revertUUID := testRevertMigration(t, uuids[len(uuids)-1], "vtctl", "vitess --allow-concurrent --postpone-completion", "rev:ctx", "", false)
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusRunning)
		// revert is running but has no --singleton-context. Our next migration should be able to run.
		uuid := testOnlineDDLStatement(t, dropNonexistentTableStatement, "vitess --allow-concurrent --singleton-context", "vtctl", "migrate:ctx", "", "", false)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, revertUUID, true)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertUUID, schema.OnlineDDLStatusCancelled)
	})
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, migrationContext string, expectHint string, expectError string, skipWait bool) (uuid string) {
	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	require.NoError(t, err)

	if executeStrategy == "vtgate" {
		assert.Empty(t, migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
		result := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, alterStatement, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true, MigrationContext: migrationContext})
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

	if !strategySetting.Strategy.IsDirect() && !skipWait && uuid != "" {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectError == "" && expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, revertUUID string, executeStrategy string, ddlStrategy string, migrationContext string, expectError string, skipWait bool) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", revertUUID)
	if executeStrategy == "vtgate" {
		assert.Empty(t, migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
		result := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, revertQuery, expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, revertQuery, cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true, MigrationContext: migrationContext})
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
