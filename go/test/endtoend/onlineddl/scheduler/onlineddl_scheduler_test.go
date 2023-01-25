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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	anyErrorIndicator = "<any-error-of-any-kind>"
)

type testOnlineDDLStatementParams struct {
	ddlStatement     string
	ddlStrategy      string
	executeStrategy  string
	expectHint       string
	expectError      string
	skipWait         bool
	migrationContext string
}

type testRevertMigrationParams struct {
	revertUUID       string
	executeStrategy  string
	ddlStrategy      string
	migrationContext string
	expectError      string
	skipWait         bool
}

var (
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams

	normalWaitTime            = 20 * time.Second
	extendedWaitTime          = 60 * time.Second
	ensureStateNotChangedTime = 5 * time.Second

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	overrideVtctlParams   *cluster.VtctlClientParams
)

func parseTableName(t *testing.T, sql string) (tableName string) {
	// ddlStatement could possibly be composed of multiple DDL statements
	tokenizer := sqlparser.NewStringTokenizer(sql)
	for {
		stmt, err := sqlparser.ParseNextStrictDDL(tokenizer)
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		ddlStmt, ok := stmt.(sqlparser.DDLStatement)
		require.True(t, ok)
		tableName = ddlStmt.GetTable().Name.String()
		if tableName == "" {
			tbls := ddlStmt.AffectedTables()
			require.NotEmpty(t, tbls)
			tableName = tbls[0].Name.String()
		}
		require.NotEmptyf(t, tableName, "could not parse table name from SQL: %s", sqlparser.String(ddlStmt))
	}
	require.NotEmptyf(t, tableName, "could not parse table name from SQL: %s", sql)
	return tableName
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func TestParseTableName(t *testing.T) {
	sqls := []string{
		`ALTER TABLE t1_test ENGINE=InnoDB`,
		`ALTER TABLE t1_test ENGINE=InnoDB;`,
		`DROP TABLE IF EXISTS t1_test`,
		`
			ALTER TABLE stress_test ENGINE=InnoDB;
			ALTER TABLE stress_test ENGINE=InnoDB;
			ALTER TABLE stress_test ENGINE=InnoDB;
		`,
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			parseTableName(t, sql)
		})
	}
}

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

	ddlStrategy := "vitess"

	createParams := func(ddlStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) *testOnlineDDLStatementParams {
		return &testOnlineDDLStatementParams{
			ddlStatement:    ddlStatement,
			ddlStrategy:     ddlStrategy,
			executeStrategy: executeStrategy,
			expectHint:      expectHint,
			expectError:     expectError,
			skipWait:        skipWait,
		}
	}

	createRevertParams := func(revertUUID string, ddlStrategy string, executeStrategy string, expectError string, skipWait bool) *testRevertMigrationParams {
		return &testRevertMigrationParams{
			revertUUID:      revertUUID,
			executeStrategy: executeStrategy,
			ddlStrategy:     ddlStrategy,
			expectError:     expectError,
			skipWait:        skipWait,
		}
	}

	mysqlVersion := onlineddl.GetMySQLVersion(t, clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet())
	require.NotEmpty(t, mysqlVersion)
	_, capableOf, _ := mysql.GetFlavor(mysqlVersion, nil)

	var (
		t1uuid string
		t2uuid string

		t1Name            = "t1_test"
		t2Name            = "t2_test"
		createT1Statement = `
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
		createT1IfNotExistsStatement = `
			CREATE TABLE IF NOT EXISTS t1_test (
				id bigint(20) not null,
				hint_col varchar(64) not null default 'should_not_appear',
				PRIMARY KEY (id)
			) ENGINE=InnoDB
		`
		trivialAlterT1Statement = `
			ALTER TABLE t1_test ENGINE=InnoDB;
		`
		trivialAlterT2Statement = `
			ALTER TABLE t2_test ENGINE=InnoDB;
		`
		instantAlterT1Statement = `
			ALTER TABLE t1_test ADD COLUMN i0 INT NOT NULL DEFAULT 0;
		`
		dropT1Statement = `
			DROP TABLE IF EXISTS t1_test
		`
		dropT3Statement = `
			DROP TABLE IF EXISTS t3_test
		`
		dropT4Statement = `
			DROP TABLE IF EXISTS t4_test
		`
	)

	testReadTimestamp := func(t *testing.T, uuid string, timestampColumn string) (timestamp string) {
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			timestamp = row.AsString(timestampColumn, "")
			assert.NotEmpty(t, timestamp)
		}
		return timestamp
	}
	testTableSequentialTimes := func(t *testing.T, uuid1, uuid2 string) {
		// expect uuid2 to start after uuid1 completes
		t.Run("Compare t1, t2 sequential times", func(t *testing.T) {
			endTime1 := testReadTimestamp(t, uuid1, "completed_timestamp")
			startTime2 := testReadTimestamp(t, uuid2, "started_timestamp")
			assert.GreaterOrEqual(t, startTime2, endTime1)
		})
	}
	testTableCompletionTimes := func(t *testing.T, uuid1, uuid2 string) {
		// expect uuid1 to complete before uuid2
		t.Run("Compare t1, t2 completion times", func(t *testing.T) {
			endTime1 := testReadTimestamp(t, uuid1, "completed_timestamp")
			endTime2 := testReadTimestamp(t, uuid2, "completed_timestamp")
			assert.GreaterOrEqual(t, endTime2, endTime1)
		})
	}
	testAllowConcurrent := func(t *testing.T, name string, uuid string, expect int64) {
		t.Run("verify allow_concurrent: "+name, func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				allowConcurrent := row.AsInt64("allow_concurrent", 0)
				assert.Equal(t, expect, allowConcurrent)
			}
		})
	}

	// CREATE
	t.Run("CREATE TABLEs t1, t1", func(t *testing.T) {
		{ // The table does not exist
			t1uuid = testOnlineDDLStatement(t, createParams(createT1Statement, ddlStrategy, "vtgate", "just-created", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		}
		{
			// The table does not exist
			t2uuid = testOnlineDDLStatement(t, createParams(createT2Statement, ddlStrategy, "vtgate", "just-created", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t2Name, true)
		}
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})
	t.Run("Postpone launch CREATE", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(createT1IfNotExistsStatement, ddlStrategy+" --postpone-launch", "vtgate", "", "", true)) // skip wait
		time.Sleep(2 * time.Second)
		rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			postponeLaunch := row.AsInt64("postpone_launch", 0)
			assert.Equal(t, int64(1), postponeLaunch)
		}
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)

		t.Run("launch all shards", func(t *testing.T) {
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, t1uuid, "", true)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(0), postponeLaunch)
			}
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("Postpone launch ALTER", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" --postpone-launch", "vtgate", "", "", true)) // skip wait
		time.Sleep(2 * time.Second)
		rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			postponeLaunch := row.AsInt64("postpone_launch", 0)
			assert.Equal(t, int64(1), postponeLaunch)
		}
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)

		t.Run("launch irrelevant UUID", func(t *testing.T) {
			someOtherUUID := "00000000_1111_2222_3333_444444444444"
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, someOtherUUID, "", false)
			time.Sleep(2 * time.Second)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(1), postponeLaunch)
			}
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)
		})
		t.Run("launch irrelevant shards", func(t *testing.T) {
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, t1uuid, "x,y,z", false)
			time.Sleep(2 * time.Second)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(1), postponeLaunch)
			}
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued)
		})
		t.Run("launch relevant shard", func(t *testing.T) {
			onlineddl.CheckLaunchMigration(t, &vtParams, shards, t1uuid, "x, y, 1", true)
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				postponeLaunch := row.AsInt64("postpone_launch", 0)
				assert.Equal(t, int64(0), postponeLaunch)
			}
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("ALTER both tables non-concurrent", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait

		t.Run("wait for t1 complete", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		})
		t.Run("wait for t1 complete", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		})
		t.Run("check both complete", func(t *testing.T) {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})
	t.Run("ALTER both tables non-concurrent, postponed", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 0)
		t.Run("expect t1 running, t2 queued", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			// now that t1 is running, let's unblock t2. We expect it to remain queued.
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			time.Sleep(ensureStateNotChangedTime)
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// non-concurrent -- should be queued!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("complete t1", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("expect t2 to complete", func(t *testing.T) {
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		testTableSequentialTimes(t, t1uuid, t2uuid)
	})
	t.Run("ALTER both tables, elligible for concurrenct", func(t *testing.T) {
		// ALTER TABLE is allowed to run concurrently when no other ALTER is busy with copy state. Our tables are tiny so we expect to find both migrations running
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" --allow-concurrent --postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" --allow-concurrent --postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "t2", t2uuid, 1)
		t.Run("expect both running", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusRunning)
		})
		t.Run("complete t2", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should still be running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		testTableCompletionTimes(t, t2uuid, t1uuid)
	})
	t.Run("ALTER both tables, elligible for concurrenct, with throttling", func(t *testing.T) {
		onlineddl.ThrottleAllMigrations(t, &vtParams)
		defer onlineddl.UnthrottleAllMigrations(t, &vtParams)
		// ALTER TABLE is allowed to run concurrently when no other ALTER is busy with copy state. Our tables are tiny so we expect to find both migrations running
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait
		t2uuid = testOnlineDDLStatement(t, createParams(trivialAlterT2Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "t2", t2uuid, 1)
		t.Run("expect t1 running", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			// since all migrations are throttled, t1 migration is not ready_to_complete, hence
			// t2 should not be running
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("unthrottle, expect t2 running", func(t *testing.T) {
			onlineddl.UnthrottleAllMigrations(t, &vtParams)
			// t1 should now be ready_to_complete, hence t2 should start running
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, extendedWaitTime, schema.OnlineDDLStatusRunning)
			time.Sleep(ensureStateNotChangedTime)
			// both should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusRunning)
		})
		t.Run("complete t2", func(t *testing.T) {
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should still be running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		testTableCompletionTimes(t, t2uuid, t1uuid)
	})
	t.Run("REVERT both tables concurrent, postponed", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))
		t2uuid = testRevertMigration(t, createRevertParams(t2uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))

		testAllowConcurrent(t, "t1", t1uuid, 1)
		t.Run("expect both migrations to run", func(t *testing.T) {
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
		})
		t.Run("test ready-to-complete", func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		t.Run("complete t2", func(t *testing.T) {
			// now that both are running, let's unblock t2. We expect it to complete.
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t2uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t2uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t2uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("concurrent REVERT vs two non-concurrent DROPs", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", true))
		drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "t1", t1uuid, 1)
		testAllowConcurrent(t, "drop3", drop3uuid, 0)
		t.Run("expect t1 migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy, "vtgate", "", "", true)) // skip wait
		t.Run("drop3 complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("drop1 blocked", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, drop1uuid, true)
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusCancelled)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
	t.Run("non-concurrent REVERT vs three concurrent drops", func(t *testing.T) {
		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -postpone-completion", "vtgate", "", true))
		drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true))                      // skip wait
		drop4uuid := testOnlineDDLStatement(t, createParams(dropT4Statement, ddlStrategy+" -allow-concurrent -postpone-completion", "vtgate", "", "", true)) // skip wait

		testAllowConcurrent(t, "drop3", drop3uuid, 1)
		t.Run("expect t1 migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true)) // skip wait
		testAllowConcurrent(t, "drop1", drop1uuid, 1)
		t.Run("t3drop complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("t1drop blocked", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		})
		t.Run("t4 postponed", func(t *testing.T) {
			// drop4 migration should postpone.
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop4uuid, schema.OnlineDDLStatusQueued)
			// Issue a complete and wait for successful completion. drop4 is non-conflicting and should be able to proceed
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, drop4uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop4uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop4uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("t1drop unblocked", func(t *testing.T) {
			// t1drop should now be unblocked!
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, false)
		})
		t.Run("revert t1 drop", func(t *testing.T) {
			revertDrop3uuid := testRevertMigration(t, createRevertParams(drop1uuid, ddlStrategy+" -allow-concurrent", "vtgate", "", true))
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertDrop3uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertDrop3uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		})
	})
	t.Run("conflicting migration does not block other queued migrations", func(t *testing.T) {
		t1uuid = testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy, "vtgate", "", "", false)) // skip wait
		t.Run("trivial t1 migration", func(t *testing.T) {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		})

		t1uuid = testRevertMigration(t, createRevertParams(t1uuid, ddlStrategy+" -postpone-completion", "vtgate", "", true))
		t.Run("expect t1 revert migration to run", func(t *testing.T) {
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
		})
		drop1uuid := testOnlineDDLStatement(t, createParams(dropT1Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", true)) // skip wait
		t.Run("t1drop blocked", func(t *testing.T) {
			time.Sleep(ensureStateNotChangedTime)
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusReady)
		})
		t.Run("t3 ready to complete", func(t *testing.T) {
			rs := onlineddl.ReadMigrations(t, &vtParams, drop1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				readyToComplete := row.AsInt64("ready_to_complete", 0)
				assert.Equal(t, int64(1), readyToComplete)
			}
		})
		t.Run("t3drop complete", func(t *testing.T) {
			// drop3 migration should not block. It can run concurrently to t1, and does not conflict
			// even though t1drop is blocked! This is the heart of this test
			drop3uuid := testOnlineDDLStatement(t, createParams(dropT3Statement, ddlStrategy+" -allow-concurrent", "vtgate", "", "", false))
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop3uuid, schema.OnlineDDLStatusComplete)
		})
		t.Run("cancel drop1", func(t *testing.T) {
			// drop1 migration should block. It can run concurrently to t1, but conflicts on table name
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusReady)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, drop1uuid, true)
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, drop1uuid, normalWaitTime, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, drop1uuid, schema.OnlineDDLStatusCancelled)
		})
		t.Run("complete t1", func(t *testing.T) {
			// t1 should be still running!
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusRunning)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})

	t.Run("Idempotent submission, retry failed migration", func(t *testing.T) {
		uuid := "00000000_1111_2222_3333_444444444444"
		overrideVtctlParams = &cluster.VtctlClientParams{DDLStrategy: ddlStrategy, SkipPreflight: true, UUIDList: uuid, MigrationContext: "idempotent:1111-2222-3333"}
		defer func() { overrideVtctlParams = nil }()
		// create a migration and cancel it. We don't let it complete. We want it in "failed" state
		t.Run("start and fail migration", func(t *testing.T) {
			executedUUID := testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy+" -postpone-completion", "vtctl", "", "", true)) // skip wait
			require.Equal(t, uuid, executedUUID)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
			// let's cancel it
			onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, true)
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusCancelled)
		})

		// now, we submit the exact same migratoin again: same UUID, same migration context.
		t.Run("resubmit migration", func(t *testing.T) {
			executedUUID := testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, ddlStrategy, "vtctl", "", "", true)) // skip wait
			require.Equal(t, uuid, executedUUID)

			// expect it to complete
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)

			rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				retries := row.AsInt64("retries", 0)
				assert.Greater(t, retries, int64(0))
			}
		})
	})
	// INSTANT DDL
	instantDDLCapable, err := capableOf(mysql.InstantAddLastColumnFlavorCapability)
	require.NoError(t, err)
	if instantDDLCapable {
		t.Run("INSTANT DDL: postpone-completion", func(t *testing.T) {
			t1uuid := testOnlineDDLStatement(t, createParams(instantAlterT1Statement, ddlStrategy+" --prefer-instant-ddl --postpone-completion", "vtgate", "", "", true))

			t.Run("expect t1 queued", func(t *testing.T) {
				// we want to validate that the migration remains queued even after some time passes. It must not move beyond 'queued'
				time.Sleep(ensureStateNotChangedTime)
				onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			})
			t.Run("complete t1", func(t *testing.T) {
				// Issue a complete and wait for successful completion
				onlineddl.CheckCompleteMigration(t, &vtParams, shards, t1uuid, true)
				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			})
		})
	}
	// 'mysql' strategy
	t.Run("mysql strategy", func(t *testing.T) {
		t.Run("declarative", func(t *testing.T) {
			t1uuid = testOnlineDDLStatement(t, createParams(createT1Statement, "mysql --declarative", "vtgate", "just-created", "", false))

			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, t1Name, true)
		})

		t.Run("fail postpone-completion", func(t *testing.T) {
			t1uuid := testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, "mysql --postpone-completion", "vtgate", "", "", true))

			// --postpone-completion not supported in mysql strategy
			time.Sleep(ensureStateNotChangedTime)
			onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusFailed)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusFailed)
		})
		t.Run("trivial", func(t *testing.T) {
			t1uuid := testOnlineDDLStatement(t, createParams(trivialAlterT1Statement, "mysql", "vtgate", "", "", true))

			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)

			rs := onlineddl.ReadMigrations(t, &vtParams, t1uuid)
			require.NotNil(t, rs)
			for _, row := range rs.Named().Rows {
				artifacts := row.AsString("artifacts", "-")
				assert.Empty(t, artifacts)
			}
		})
		t.Run("instant", func(t *testing.T) {
			t1uuid := testOnlineDDLStatement(t, createParams(instantAlterT1Statement, "mysql", "vtgate", "", "", true))

			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, t1uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, t1uuid, schema.OnlineDDLStatusComplete)
		})
	})
}

func TestSingleton(t *testing.T) {
	defer cluster.PanicHandler(t)

	require.Equal(t, 1, len(shards))

	createParams := func(ddlStatement string, ddlStrategy string, executeStrategy string, migrationContext string, expectHint string, expectError string, skipWait bool) *testOnlineDDLStatementParams {
		return &testOnlineDDLStatementParams{
			ddlStatement:     ddlStatement,
			ddlStrategy:      ddlStrategy,
			executeStrategy:  executeStrategy,
			migrationContext: migrationContext,
			expectHint:       expectHint,
			expectError:      expectError,
			skipWait:         skipWait,
		}
	}

	createRevertParams := func(revertUUID string, ddlStrategy string, executeStrategy string, migrationContext string, expectError string, skipWait bool) *testRevertMigrationParams {
		return &testRevertMigrationParams{
			revertUUID:       revertUUID,
			executeStrategy:  executeStrategy,
			ddlStrategy:      ddlStrategy,
			migrationContext: migrationContext,
			expectError:      expectError,
			skipWait:         skipWait,
		}
	}

	var (
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

	var uuids []string
	// CREATE
	t.Run("CREATE TABLE", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatement(t, createParams(createStatement, onlineSingletonDDLStrategy, "vtgate", "", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})
	t.Run("revert CREATE TABLE", func(t *testing.T) {
		// The table existed, so it will now be dropped (renamed)
		uuid := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], onlineSingletonDDLStrategy, "vtgate", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert revert CREATE TABLE", func(t *testing.T) {
		// Table was dropped (renamed) so it will now be restored
		uuid := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], onlineSingletonDDLStrategy, "vtgate", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	var throttledUUID string
	t.Run("throttled migration", func(t *testing.T) {
		throttledUUID = testOnlineDDLStatement(t, createParams(alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtgate", "", "hint_col", "", false))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, throttledUUID, schema.OnlineDDLStatusRunning)
	})
	t.Run("failed singleton migration, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtgate", "", "hint_col", "rejected", true))
		assert.Empty(t, uuid)
	})
	t.Run("failed singleton migration, vtctl", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(alterTableThrottlingStatement, "gh-ost --singleton --max-load=Threads_running=1", "vtctl", "", "hint_col", "rejected", true))
		assert.Empty(t, uuid)
	})
	t.Run("failed revert migration", func(t *testing.T) {
		uuid := testRevertMigration(t, createRevertParams(throttledUUID, onlineSingletonDDLStrategy, "vtgate", "", "rejected", true))
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
		uuid := testOnlineDDLStatement(t, createParams(alterTableTrivialStatement, "gh-ost --singleton", "vtctl", "", "hint_col", "", false))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})
	t.Run("successful gh-ost alter, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(alterTableTrivialStatement, "gh-ost --singleton", "vtgate", "", "hint_col", "", false))
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
	})

	t.Run("successful online alter, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(alterTableTrivialStatement, onlineSingletonDDLStrategy, "vtgate", "", "hint_col", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, uuid, false)
		onlineddl.CheckRetryMigration(t, &vtParams, shards, uuid, false)
		checkTable(t, tableName, true)
	})
	t.Run("revert ALTER TABLE, vttablet", func(t *testing.T) {
		// The table existed, so it will now be dropped (renamed)
		uuid := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], onlineSingletonDDLStrategy, "vtctl", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	var throttledUUIDs []string
	// singleton-context
	t.Run("throttled migrations, singleton-context", func(t *testing.T) {
		uuidList := testOnlineDDLStatement(t, createParams(multiAlterTableThrottlingStatement, "gh-ost --singleton-context --max-load=Threads_running=1", "vtctl", "", "hint_col", "", false))
		throttledUUIDs = strings.Split(uuidList, "\n")
		assert.Equal(t, 3, len(throttledUUIDs))
		for _, uuid := range throttledUUIDs {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady, schema.OnlineDDLStatusRunning)
		}
	})
	t.Run("failed migrations, singleton-context", func(t *testing.T) {
		_ = testOnlineDDLStatement(t, createParams(multiAlterTableThrottlingStatement, "gh-ost --singleton-context --max-load=Threads_running=1", "vtctl", "", "hint_col", "rejected", false))
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
		uuidList := testOnlineDDLStatement(t, createParams(multiDropStatements, onlineSingletonContextDDLStrategy, "vtctl", "", "", "", false))
		uuidSlice := strings.Split(uuidList, "\n")
		assert.Equal(t, 3, len(uuidSlice))
		for _, uuid := range uuidSlice {
			uuid = strings.TrimSpace(uuid)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		}
	})

	//DROP

	t.Run("online DROP TABLE", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(dropStatement, onlineSingletonDDLStrategy, "vtgate", "", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE", func(t *testing.T) {
		// This will recreate the table (well, actually, rename it back into place)
		uuid := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], onlineSingletonDDLStrategy, "vttablet", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})

	t.Run("fail concurrent singleton, vtgate", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, createParams(alterTableTrivialStatement, "vitess --postpone-completion --singleton", "vtgate", "", "hint_col", "", true))
		uuids = append(uuids, uuid)
		_ = testOnlineDDLStatement(t, createParams(dropNonexistentTableStatement, "vitess --singleton", "vtgate", "", "hint_col", "rejected", true))
		onlineddl.CheckCompleteAllMigrations(t, &vtParams, len(shards))
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
	})
	t.Run("fail concurrent singleton-context with revert", func(t *testing.T) {
		revertUUID := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], "vitess --allow-concurrent --postpone-completion --singleton-context", "vtctl", "rev:ctx", "", false))
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusRunning)
		// revert is running
		_ = testOnlineDDLStatement(t, createParams(dropNonexistentTableStatement, "vitess --allow-concurrent --singleton-context", "vtctl", "migrate:ctx", "", "rejected", true))
		onlineddl.CheckCancelMigration(t, &vtParams, shards, revertUUID, true)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertUUID, schema.OnlineDDLStatusCancelled)
	})
	t.Run("success concurrent singleton-context with no-context revert", func(t *testing.T) {
		revertUUID := testRevertMigration(t, createRevertParams(uuids[len(uuids)-1], "vitess --allow-concurrent --postpone-completion", "vtctl", "rev:ctx", "", false))
		onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusRunning)
		// revert is running but has no --singleton-context. Our next migration should be able to run.
		uuid := testOnlineDDLStatement(t, createParams(dropNonexistentTableStatement, "vitess --allow-concurrent --singleton-context", "vtctl", "migrate:ctx", "", "", false))
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckCancelMigration(t, &vtParams, shards, revertUUID, true)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, revertUUID, 20*time.Second, schema.OnlineDDLStatusFailed, schema.OnlineDDLStatusCancelled)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, revertUUID, schema.OnlineDDLStatusCancelled)
	})
}
func TestForeignKeys(t *testing.T) {
	defer cluster.PanicHandler(t)

	var (
		createStatements = []string{
			`
			CREATE TABLE parent_table (
					id INT NOT NULL,
					parent_hint_col INT NOT NULL DEFAULT 0,
					PRIMARY KEY (id)
			)
		`,
			`
			CREATE TABLE child_table (
				id INT NOT NULL auto_increment,
				parent_id INT,
				child_hint_col INT NOT NULL DEFAULT 0,
				PRIMARY KEY (id),
				KEY parent_id_idx (parent_id),
				CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE
			)
		`,
			`
			CREATE TABLE child_nofk_table (
				id INT NOT NULL auto_increment,
				parent_id INT,
				child_hint_col INT NOT NULL DEFAULT 0,
				PRIMARY KEY (id),
				KEY parent_id_idx (parent_id)
			)
		`,
		}
		insertStatements = []string{
			"insert into parent_table (id) values(43)",
			"insert into child_table (id, parent_id) values(1,43)",
			"insert into child_table (id, parent_id) values(2,43)",
			"insert into child_table (id, parent_id) values(3,43)",
			"insert into child_table (id, parent_id) values(4,43)",
		}
		ddlStrategy        = "online --allow-zero-in-date"
		ddlStrategyAllowFK = ddlStrategy + " --unsafe-allow-foreign-keys"
	)

	type testCase struct {
		name             string
		sql              string
		allowForeignKeys bool
		expectHint       string
	}
	var testCases = []testCase{
		{
			name:             "modify parent, not allowed",
			sql:              "alter table parent_table engine=innodb",
			allowForeignKeys: false,
		},
		{
			name:             "modify child, not allowed",
			sql:              "alter table child_table engine=innodb",
			allowForeignKeys: false,
		},
		{
			name:             "add foreign key to child, not allowed",
			sql:              "alter table child_table add CONSTRAINT another_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
			allowForeignKeys: false,
		},
		{
			name:             "add foreign key to table which wasn't a child before, not allowed",
			sql:              "alter table child_nofk_table add CONSTRAINT new_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
			allowForeignKeys: false,
		},
		{
			// on vanilla MySQL, this migration ends with the child_table referencing the old, original table, and not to the new table now called parent_table.
			// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
			// However, this tests is still valid in the sense that it lets us modify the parent table in the first place.
			name:             "modify parent, trivial",
			sql:              "alter table parent_table engine=innodb",
			allowForeignKeys: true,
			expectHint:       "parent_hint_col",
		},
		{
			// on vanilla MySQL, this migration ends with two tables, the original and the new child_table, both referencing parent_table. This has
			// the unwanted property of then limiting actions on the parent_table based on what rows exist or do not exist on the now stale old
			// child table.
			// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
			// However, this tests is still valid in the sense that it lets us modify the child table in the first place.
			// A valid use case: using FOREIGN_KEY_CHECKS=0  at all times.
			name:             "modify child, trivial",
			sql:              "alter table child_table engine=innodb",
			allowForeignKeys: true,
			expectHint:       "REFERENCES `parent_table`",
		},
		{
			// on vanilla MySQL, this migration ends with two tables, the original and the new child_table, both referencing parent_table. This has
			// the unwanted property of then limiting actions on the parent_table based on what rows exist or do not exist on the now stale old
			// child table.
			// This is a fundamental foreign key limitation, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/
			// However, this tests is still valid in the sense that it lets us modify the child table in the first place.
			// A valid use case: using FOREIGN_KEY_CHECKS=0  at all times.
			name:             "add foreign key to child",
			sql:              "alter table child_table add CONSTRAINT another_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
			allowForeignKeys: true,
			expectHint:       "another_fk",
		},
		{
			name:             "add foreign key to table which wasn't a child before",
			sql:              "alter table child_nofk_table add CONSTRAINT new_fk FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE",
			allowForeignKeys: true,
			expectHint:       "new_fk",
		},
	}

	createParams := func(ddlStatement string, ddlStrategy string, executeStrategy string, expectHint string, expectError string, skipWait bool) *testOnlineDDLStatementParams {
		return &testOnlineDDLStatementParams{
			ddlStatement:    ddlStatement,
			ddlStrategy:     ddlStrategy,
			executeStrategy: executeStrategy,
			expectHint:      expectHint,
			expectError:     expectError,
			skipWait:        skipWait,
		}
	}

	testStatement := func(t *testing.T, sql string, ddlStrategy string, expectHint string, expectError bool) (uuid string) {
		errorHint := ""
		if expectError {
			errorHint = anyErrorIndicator
		}
		return testOnlineDDLStatement(t, createParams(sql, ddlStrategy, "vtctl", expectHint, errorHint, false))
	}
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			t.Run("create tables", func(t *testing.T) {
				for _, statement := range createStatements {
					t.Run(statement, func(t *testing.T) {
						uuid := testStatement(t, statement, ddlStrategyAllowFK, "", false)
						onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
					})
				}
			})
			t.Run("populate tables", func(t *testing.T) {
				for _, statement := range insertStatements {
					t.Run(statement, func(t *testing.T) {
						onlineddl.VtgateExecQuery(t, &vtParams, statement, "")
					})
				}
			})
			var uuid string
			t.Run("run migration", func(t *testing.T) {
				if testcase.allowForeignKeys {
					uuid = testStatement(t, testcase.sql, ddlStrategyAllowFK, testcase.expectHint, false)
					onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				} else {
					uuid = testStatement(t, testcase.sql, ddlStrategy, "", true)
					if uuid != "" {
						onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
					}
				}
			})
			t.Run("cleanup", func(t *testing.T) {
				var artifacts []string
				if uuid != "" {
					rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
					require.NotNil(t, rs)
					row := rs.Named().Row()
					require.NotNil(t, row)

					artifacts = textutil.SplitDelimitedList(row.AsString("artifacts", ""))
				}

				artifacts = append(artifacts, "child_table", "child_nofk_table", "parent_table")
				// brute force drop all tables. In MySQL 8.0 you can do a single `DROP TABLE ... <list of all tables>`
				// which auto-resovled order. But in 5.7 you can't.
				droppedTables := map[string]bool{}
				for range artifacts {
					for _, artifact := range artifacts {
						if droppedTables[artifact] {
							continue
						}
						statement := fmt.Sprintf("DROP TABLE IF EXISTS %s", artifact)
						_, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, statement, cluster.VtctlClientParams{DDLStrategy: "direct", SkipPreflight: true})
						if err == nil {
							droppedTables[artifact] = true
						}
					}
				}
				statement := fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.Join(artifacts, ","))
				t.Run(statement, func(t *testing.T) {
					testStatement(t, statement, "direct", "", false)
				})
			})
		})
	}
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, params *testOnlineDDLStatementParams) (uuid string) {
	strategySetting, err := schema.ParseDDLStrategy(params.ddlStrategy)
	require.NoError(t, err)

	tableName := parseTableName(t, params.ddlStatement)

	if params.executeStrategy == "vtgate" {
		require.Empty(t, params.migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
		result := onlineddl.VtgateExecDDL(t, &vtParams, params.ddlStrategy, params.ddlStatement, params.expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		vtctlParams := &cluster.VtctlClientParams{DDLStrategy: params.ddlStrategy, MigrationContext: params.migrationContext, SkipPreflight: true}
		if overrideVtctlParams != nil {
			vtctlParams = overrideVtctlParams
		}
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, params.ddlStatement, *vtctlParams)
		switch params.expectError {
		case anyErrorIndicator:
			if err != nil {
				// fine. We got any error.
				t.Logf("expected any error, got this error: %v", err)
				return
			}
			uuid = output
		case "":
			assert.NoError(t, err)
			uuid = output
		default:
			assert.Error(t, err)
			assert.Contains(t, output, params.expectError)
		}
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	if !strategySetting.Strategy.IsDirect() && !params.skipWait && uuid != "" {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if params.expectError == "" && params.expectHint != "" {
		checkMigratedTable(t, tableName, params.expectHint)
	}
	return uuid
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, params *testRevertMigrationParams) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", params.revertUUID)
	if params.executeStrategy == "vtgate" {
		require.Empty(t, params.migrationContext, "explicit migration context not supported in vtgate. Test via vtctl")
		result := onlineddl.VtgateExecDDL(t, &vtParams, params.ddlStrategy, revertQuery, params.expectError)
		if result != nil {
			row := result.Named().Row()
			if row != nil {
				uuid = row.AsString("uuid", "")
			}
		}
	} else {
		output, err := clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, revertQuery, cluster.VtctlClientParams{DDLStrategy: params.ddlStrategy, MigrationContext: params.migrationContext, SkipPreflight: true})
		if params.expectError == "" {
			assert.NoError(t, err)
			uuid = output
		} else {
			assert.Error(t, err)
			assert.Contains(t, output, params.expectError)
		}
	}

	if params.expectError == "" {
		uuid = strings.TrimSpace(uuid)
		fmt.Println("# Generated UUID (for debug purposes):")
		fmt.Printf("<%s>\n", uuid)
	}
	if !params.skipWait {
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
