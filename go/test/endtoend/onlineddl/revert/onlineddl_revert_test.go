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

package revert

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	clusterInstance *cluster.LocalProcessCluster
	shards          []cluster.Shard
	vtParams        mysql.ConnParams
	mysqlVersion    string

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	viewBaseTableName     = `view_base_table_test`
	viewName              = `view_test`
	insertRowStatement    = `
		INSERT IGNORE INTO stress_test (id, rand_val) VALUES (%d, left(md5(rand()), 8))
	`
	updateRowStatement = `
		UPDATE stress_test SET updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d AND updates=1
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

const (
	maxTableRows   = 4096
	maxConcurrency = 5
)

type revertibleTestCase struct {
	name       string
	fromSchema string
	toSchema   string
	// expectProblems              bool
	removedUniqueKeyNames       string
	droppedNoDefaultColumnNames string
	expandedColumnNames         string
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
			"--schema_change_check_interval", "1s",
		}

		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "5s",
			"--queryserver-config-schema-change-signal-interval", "0.1",
			"--watch_replication_stream",
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

	t.Run("revertible", testRevertible)
	t.Run("revert", testRevert)
}

func testRevertible(t *testing.T) {

	var testCases = []revertibleTestCase{
		{
			name:       "identical schemas",
			fromSchema: `id int primary key, i1 int not null default 0`,
			toSchema:   `id int primary key, i2 int not null default 0`,
		},
		{
			name:       "different schemas, nothing to note",
			fromSchema: `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
			toSchema:   `id int primary key, i1 int not null default 0, i2 int not null default 0, unique key i1_uidx(i1)`,
		},
		{
			name:                  "removed non-nullable unique key",
			fromSchema:            `id int primary key, i1 int not null default 0, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i2 int not null default 0`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "removed nullable unique key",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i2 int default null`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "expanding unique key removes unique constraint",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
			removedUniqueKeyNames: `i1_uidx`,
		},
		{
			name:                  "reducing unique key does not remove unique constraint",
			fromSchema:            `id int primary key, i1 int default null, unique key i1_uidx(i1, id)`,
			toSchema:              `id int primary key, i1 int default null, unique key i1_uidx(i1)`,
			removedUniqueKeyNames: ``,
		},
		{
			name:                        "remove column without default",
			fromSchema:                  `id int primary key, i1 int not null`,
			toSchema:                    `id int primary key, i2 int not null default 0`,
			droppedNoDefaultColumnNames: `i1`,
		},
		{
			name:                "expanded: nullable",
			fromSchema:          `id int primary key, i1 int not null, i2 int default null`,
			toSchema:            `id int primary key, i1 int default null, i2 int not null`,
			expandedColumnNames: `i1`,
		},
		{
			name:                "expanded: longer text",
			fromSchema:          `id int primary key, i1 int default null, v1 varchar(40) not null, v2 varchar(5), v3 varchar(3)`,
			toSchema:            `id int primary key, i1 int not null, v1 varchar(100) not null, v2 char(3), v3 char(5)`,
			expandedColumnNames: `v1,v3`,
		},
		{
			name:                "expanded: int numeric precision and scale",
			fromSchema:          `id int primary key, i1 int, i2 tinyint, i3 mediumint, i4 bigint`,
			toSchema:            `id int primary key, i1 int, i2 mediumint, i3 int, i4 tinyint`,
			expandedColumnNames: `i2,i3`,
		},
		{
			name:                "expanded: floating point",
			fromSchema:          `id int primary key, i1 int, n2 bigint, n3 bigint, n4 float, n5 double`,
			toSchema:            `id int primary key, i1 int, n2 float, n3 double, n4 double, n5 float`,
			expandedColumnNames: `n2,n3,n4`,
		},
		{
			name:                "expanded: decimal numeric precision and scale",
			fromSchema:          `id int primary key, i1 int, d1 decimal(10,2), d2 decimal (10,2), d3 decimal (10,2)`,
			toSchema:            `id int primary key, i1 int, d1 decimal(11,2), d2 decimal (9,1), d3 decimal (10,3)`,
			expandedColumnNames: `d1,d3`,
		},
		{
			name:                "expanded: signed, unsigned",
			fromSchema:          `id int primary key, i1 bigint signed, i2 int unsigned, i3 bigint unsigned`,
			toSchema:            `id int primary key, i1 int signed, i2 int signed, i3 int signed`,
			expandedColumnNames: `i2,i3`,
		},
		{
			name:                "expanded: signed, unsigned: range",
			fromSchema:          `id int primary key, i1 int signed, i2 bigint signed, i3 int signed`,
			toSchema:            `id int primary key, i1 int unsigned, i2 int unsigned, i3 bigint unsigned`,
			expandedColumnNames: `i1,i3`,
		},
		{
			name:                "expanded: datetime precision",
			fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, ti1 time, dt2 datetime(3), dt3 datetime(6), ts2 timestamp(3)`,
			toSchema:            `id int primary key, dt1 datetime(3), ts1 timestamp(6), ti1 time(3), dt2 datetime(6), dt3 datetime(3), ts2 timestamp`,
			expandedColumnNames: `dt1,ts1,ti1,dt2`,
		},
		{
			name:                "expanded: strange data type changes",
			fromSchema:          `id int primary key, dt1 datetime, ts1 timestamp, i1 int, d1 date, e1 enum('a', 'b')`,
			toSchema:            `id int primary key, dt1 char(32), ts1 varchar(32), i1 tinytext, d1 char(2), e1 varchar(2)`,
			expandedColumnNames: `dt1,ts1,i1,d1,e1`,
		},
		{
			name:                "expanded: temporal types",
			fromSchema:          `id int primary key, t1 time, t2 timestamp, t3 date, t4 datetime, t5 time, t6 date`,
			toSchema:            `id int primary key, t1 datetime, t2 datetime, t3 timestamp, t4 timestamp, t5 timestamp, t6 datetime`,
			expandedColumnNames: `t1,t2,t3,t5,t6`,
		},
		{
			name:                "expanded: character sets",
			fromSchema:          `id int primary key, c1 char(3) charset utf8, c2 char(3) charset utf8mb4, c3 char(3) charset ascii, c4 char(3) charset utf8mb4, c5 char(3) charset utf8, c6 char(3) charset latin1`,
			toSchema:            `id int primary key, c1 char(3) charset utf8mb4, c2 char(3) charset utf8, c3 char(3) charset utf8, c4 char(3) charset ascii, c5 char(3) charset utf8, c6 char(3) charset utf8mb4`,
			expandedColumnNames: `c1,c3,c6`,
		},
		{
			name:                "expanded: enum",
			fromSchema:          `id int primary key, e1 enum('a', 'b'), e2 enum('a', 'b'), e3 enum('a', 'b'), e4 enum('a', 'b'), e5 enum('a', 'b'), e6 enum('a', 'b'), e7 enum('a', 'b'), e8 enum('a', 'b')`,
			toSchema:            `id int primary key, e1 enum('a', 'b'), e2 enum('a'), e3 enum('a', 'b', 'c'), e4 enum('a', 'x'), e5 enum('a', 'x', 'b'), e6 enum('b'), e7 varchar(1), e8 tinyint`,
			expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
		},
		{
			name:                "expanded: set",
			fromSchema:          `id int primary key, e1 set('a', 'b'), e2 set('a', 'b'), e3 set('a', 'b'), e4 set('a', 'b'), e5 set('a', 'b'), e6 set('a', 'b'), e7 set('a', 'b'), e8 set('a', 'b')`,
			toSchema:            `id int primary key, e1 set('a', 'b'), e2 set('a'), e3 set('a', 'b', 'c'), e4 set('a', 'x'), e5 set('a', 'x', 'b'), e6 set('b'), e7 varchar(1), e8 tinyint`,
			expandedColumnNames: `e3,e4,e5,e6,e7,e8`,
		},
	}

	var (
		createTableWrapper = `CREATE TABLE onlineddl_test(%s)`
		dropTableStatement = `
			DROP TABLE onlineddl_test
		`
		tableName   = "onlineddl_test"
		ddlStrategy = "online --declarative --allow-zero-in-date"
	)

	removeBackticks := func(s string) string {
		return strings.Replace(s, "`", "", -1)
	}

	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {

			t.Run("ensure table dropped", func(t *testing.T) {
				// A preparation step, to clean up anything from the previous test case
				uuid := testOnlineDDLStatement(t, dropTableStatement, ddlStrategy, "vtgate", tableName, "")
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, false)
			})

			t.Run("create from-table", func(t *testing.T) {
				// A preparation step, to re-create the base table
				fromStatement := fmt.Sprintf(createTableWrapper, testcase.fromSchema)
				uuid := testOnlineDDLStatement(t, fromStatement, ddlStrategy, "vtgate", tableName, "")
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, true)
			})
			var uuid string
			t.Run("run migration", func(t *testing.T) {
				// This is the migration we will test, and see whether it is revertible or not (and why not).
				toStatement := fmt.Sprintf(createTableWrapper, testcase.toSchema)
				uuid = testOnlineDDLStatement(t, toStatement, ddlStrategy, "vtgate", tableName, "")
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
				checkTable(t, tableName, true)
			})
			t.Run("check migration", func(t *testing.T) {
				// All right, the actual test
				rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
				require.NotNil(t, rs)
				for _, row := range rs.Named().Rows {
					removedUniqueKeyNames := row.AsString("removed_unique_key_names", "")
					droppedNoDefaultColumnNames := row.AsString("dropped_no_default_column_names", "")
					expandedColumnNames := row.AsString("expanded_column_names", "")

					assert.Equal(t, testcase.removedUniqueKeyNames, removeBackticks(removedUniqueKeyNames))
					assert.Equal(t, testcase.droppedNoDefaultColumnNames, removeBackticks(droppedNoDefaultColumnNames))
					assert.Equal(t, testcase.expandedColumnNames, removeBackticks(expandedColumnNames))
				}
			})
		})
	}
}

func testRevert(t *testing.T) {

	var (
		partitionedTableName = `part_test`
		createStatement      = `
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
		createIfNotExistsStatement = `
		CREATE TABLE IF NOT EXISTS stress_test (
			id bigint(20) not null,
			PRIMARY KEY (id)
		) ENGINE=InnoDB
	`
		dropStatement = `
		DROP TABLE stress_test
	`
		dropIfExistsStatement = `
		DROP TABLE IF EXISTS stress_test
	`
		alterHintStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default '%s'
	`
		createViewBaseTableStatement = `
		CREATE TABLE view_base_table_test (id INT PRIMARY KEY)
	`
		createViewStatement = `
		CREATE VIEW view_test AS SELECT 'success_create' AS msg FROM view_base_table_test
	`
		createOrReplaceViewStatement = `
		CREATE OR REPLACE VIEW view_test AS SELECT 'success_replace' AS msg FROM view_base_table_test
	`
		alterViewStatement = `
		ALTER VIEW view_test AS SELECT 'success_alter' AS msg FROM view_base_table_test
	`
		dropViewStatement = `
		DROP VIEW view_test
	`
		dropViewIfExistsStatement = `
		DROP VIEW IF EXISTS view_test
	`
		createPartitionedTableStatement = `
		CREATE TABLE part_test (
			id INT NOT NULL,
			ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			primary key (id)
		)
		PARTITION BY RANGE (id) (
				PARTITION p1 VALUES LESS THAN (10),
				PARTITION p2 VALUES LESS THAN (20),
				PARTITION p3 VALUES LESS THAN (30),
				PARTITION p4 VALUES LESS THAN (40),
				PARTITION p5 VALUES LESS THAN (50),
				PARTITION p6 VALUES LESS THAN (60)
		)
	`
		populatePartitionedTableStatement = `
		INSERT INTO part_test (id) VALUES (2),(11),(23),(37),(41),(53)
	`
	)

	populatePartitionedTable := func(t *testing.T) {
		onlineddl.VtgateExecQuery(t, &vtParams, populatePartitionedTableStatement, "")
	}

	mysqlVersion = onlineddl.GetMySQLVersion(t, clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet())
	require.NotEmpty(t, mysqlVersion)

	_, capableOf, _ := mysql.GetFlavor(mysqlVersion, nil)

	var uuids []string
	ddlStrategy := "online"

	testRevertedUUID := func(t *testing.T, uuid string, expectRevertedUUID string) {
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		for _, row := range rs.Named().Rows {
			revertedUUID := row["reverted_uuid"].ToString()
			assert.Equal(t, expectRevertedUUID, revertedUUID)
		}
	}

	t.Run("create base table for view", func(t *testing.T) {
		uuid := testOnlineDDLStatementForView(t, createViewBaseTableStatement, ddlStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewBaseTableName, true)
		testRevertedUUID(t, uuid, "")
	})

	// CREATE VIEW

	t.Run("CREATE VIEW where view does not exist", func(t *testing.T) {
		// The view does not exist
		uuid := testOnlineDDLStatementForView(t, createViewStatement, ddlStrategy, "vtgate", "success_create")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		testRevertedUUID(t, uuid, "")
	})
	t.Run("revert CREATE VIEW where view does not exist", func(t *testing.T) {
		// The view was created, so it will now be dropped (renamed)
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("revert revert CREATE VIEW where view does not exist", func(t *testing.T) {
		// View was dropped (renamed) so it will now be restored
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		testRevertedUUID(t, uuid, revertedUUID)
	})

	// CREATE OR REPLACE VIEW
	t.Run("CREATE PR REPLACE VIEW where view exists", func(t *testing.T) {
		// The view exists
		uuid := testOnlineDDLStatementForView(t, createOrReplaceViewStatement, ddlStrategy, "vtgate", "success_replace")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		testRevertedUUID(t, uuid, "")
	})
	t.Run("revert CREATE PR REPLACE VIEW where view exists", func(t *testing.T) {
		// Restore original view
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		checkMigratedTable(t, viewName, "success_create")
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("revert revert CREATE PR REPLACE VIEW where view exists", func(t *testing.T) {
		// View was dropped (renamed) so it will now be restored
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		checkMigratedTable(t, viewName, "success_replace")
		testRevertedUUID(t, uuid, revertedUUID)
	})

	// ALTER VIEW
	t.Run("ALTER VIEW where view exists", func(t *testing.T) {
		// The view exists
		checkTable(t, viewName, true)
		uuid := testOnlineDDLStatementForView(t, alterViewStatement, ddlStrategy, "vtgate", "success_alter")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		testRevertedUUID(t, uuid, "")
	})
	t.Run("revert ALTER VIEW where view exists", func(t *testing.T) {
		// Restore original view
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		checkMigratedTable(t, viewName, "success_replace")
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("revert revert ALTER VIEW where view exists", func(t *testing.T) {
		// View was dropped (renamed) so it will now be restored
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
		checkMigratedTable(t, viewName, "success_alter")
		testRevertedUUID(t, uuid, revertedUUID)
	})

	// DROP VIEW

	t.Run("online DROP VIEW", func(t *testing.T) {
		// view exists
		uuid := testOnlineDDLStatementForTable(t, dropViewStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})
	t.Run("ALTER VIEW where view does not exist", func(t *testing.T) {
		// The view does not exist. Expect failure
		uuid := testOnlineDDLStatementForView(t, alterViewStatement, ddlStrategy, "vtgate", "")
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkTable(t, viewName, false)
	})

	t.Run("revert DROP VIEW", func(t *testing.T) {
		// This will recreate the view (well, actually, rename it back into place)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, true)
	})
	t.Run("revert revert DROP VIEW", func(t *testing.T) {
		// This will reapply DROP VIEW
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})
	t.Run("fail online DROP VIEW", func(t *testing.T) {
		// The view does now exist
		uuid := testOnlineDDLStatementForTable(t, dropViewStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkTable(t, viewName, false)
	})

	// DROP VIEW IF EXISTS
	t.Run("online DROP VIEW IF EXISTS", func(t *testing.T) {
		// The view doesn't actually exist right now
		uuid := testOnlineDDLStatementForTable(t, dropViewIfExistsStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})
	t.Run("revert DROP VIEW IF EXISTS", func(t *testing.T) {
		// View will not be recreated because it didn't exist during the DROP VIEW IF EXISTS
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})
	t.Run("revert revert DROP VIEW IF EXISTS", func(t *testing.T) {
		// View still does not exist
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})
	t.Run("revert revert revert DROP VIEW IF EXISTS", func(t *testing.T) {
		// View still does not exist
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, viewName, false)
	})

	// CREATE
	t.Run("CREATE TABLE IF NOT EXISTS where table does not exist", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatementForTable(t, createIfNotExistsStatement, ddlStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		testRevertedUUID(t, uuid, "")
	})
	t.Run("revert CREATE TABLE IF NOT EXISTS where did not exist", func(t *testing.T) {
		// The table was created, so it will now be dropped (renamed)
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("revert revert CREATE TABLE IF NOT EXISTS where did not exist", func(t *testing.T) {
		// Table was dropped (renamed) so it will now be restored
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("revert revert revert CREATE TABLE IF NOT EXISTS where did not exist", func(t *testing.T) {
		// Table was restored, so it will now be dropped (renamed)
		revertedUUID := uuids[len(uuids)-1]
		uuid := testRevertMigration(t, revertedUUID, ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
		testRevertedUUID(t, uuid, revertedUUID)
	})
	t.Run("online CREATE TABLE", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, createStatement, ddlStrategy, "vtgate", "just-created")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		initTable(t)
		testSelectTableMetrics(t)
		testRevertedUUID(t, uuid, "")
	})
	t.Run("revert CREATE TABLE", func(t *testing.T) {
		// This will drop the table (well, actually, rename it away)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert revert CREATE TABLE", func(t *testing.T) {
		// Restore the table. Data should still be in the table!
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("fail revert older change", func(t *testing.T) {
		// We shouldn't be able to revert one-before-last succcessful migration.
		uuid := testRevertMigration(t, uuids[len(uuids)-2], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
	})
	t.Run("CREATE TABLE IF NOT EXISTS where table exists", func(t *testing.T) {
		// The table exists. A noop.
		uuid := testOnlineDDLStatementForTable(t, createIfNotExistsStatement, ddlStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})
	t.Run("revert CREATE TABLE IF NOT EXISTS where table existed", func(t *testing.T) {
		// Since the table already existed, thus not created by the reverts migration,
		// we expect to _not_ drop it in this revert. A noop.
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})
	t.Run("revert revert CREATE TABLE IF NOT EXISTS where table existed", func(t *testing.T) {
		// Table was not dropped, thus isn't re-created, and it just still exists. A noop.
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
	})
	t.Run("fail online CREATE TABLE", func(t *testing.T) {
		// Table already exists
		uuid := testOnlineDDLStatementForTable(t, createStatement, "online", "vtgate", "just-created")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkTable(t, tableName, true)
	})

	// ALTER

	// Run two ALTER TABLE statements.
	// These tests are similar to `onlineddl_vrepl_stress` endtond tests.
	// If they fail, it has nothing to do with revert.
	// We run these tests because we expect their functionality to work in the next step.
	var alterHints []string
	for i := 0; i < 2; i++ {
		testName := fmt.Sprintf("online ALTER TABLE %d", i)
		hint := fmt.Sprintf("hint-alter-%d", i)
		alterHints = append(alterHints, hint)
		t.Run(testName, func(t *testing.T) {
			// One alter. We're not going to revert it.
			// This specific test is similar to `onlineddl_vrepl_stress` endtond tests.
			// If it fails, it has nothing to do with revert.
			// We run this test because we expect its functionality to work in the next step.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				runMultipleConnections(ctx, t)
			}()
			uuid := testOnlineDDLStatementForTable(t, fmt.Sprintf(alterHintStatement, hint), "online", "vtgate", hint)
			uuids = append(uuids, uuid)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			cancel() // will cause runMultipleConnections() to terminate
			wg.Wait()
			testSelectTableMetrics(t)
		})
	}
	t.Run("revert ALTER TABLE", func(t *testing.T) {
		// This reverts the last ALTER TABLE.
		// And we run traffic on the table during the revert
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			runMultipleConnections(ctx, t)
		}()
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		cancel() // will cause runMultipleConnections() to terminate
		wg.Wait()
		checkMigratedTable(t, tableName, alterHints[0])
		testSelectTableMetrics(t)
	})
	t.Run("revert revert ALTER TABLE", func(t *testing.T) {
		// This reverts the last revert (reapplying the last ALTER TABLE).
		// And we run traffic on the table during the revert
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			runMultipleConnections(ctx, t)
		}()
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		cancel() // will cause runMultipleConnections() to terminate
		wg.Wait()
		checkMigratedTable(t, tableName, alterHints[1])
		testSelectTableMetrics(t)
	})
	t.Run("revert revert revert ALTER TABLE", func(t *testing.T) {
		// For good measure, let's verify that revert-revert-revert works...
		// So this again pulls us back to first ALTER
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			runMultipleConnections(ctx, t)
		}()
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		cancel() // will cause runMultipleConnections() to terminate
		wg.Wait()
		checkMigratedTable(t, tableName, alterHints[0])
		testSelectTableMetrics(t)
	})
	testPostponedRevert := func(t *testing.T, expectStatuses ...schema.OnlineDDLStatus) {
		require.NotEmpty(t, expectStatuses)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			runMultipleConnections(ctx, t)
		}()
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy+" --postpone-completion")
		uuids = append(uuids, uuid)
		// Should be still running!
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, expectStatuses...)
		// Issue a complete and wait for successful completion
		onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
		// This part may take a while, because we depend on vreplication polling
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 60*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		cancel() // will cause runMultipleConnections() to terminate
		wg.Wait()
	}
	t.Run("postponed revert", func(t *testing.T) {
		testPostponedRevert(t, schema.OnlineDDLStatusRunning)
		checkMigratedTable(t, tableName, alterHints[1])
		testSelectTableMetrics(t)
	})

	t.Run("postponed revert view", func(t *testing.T) {
		t.Run("CREATE VIEW again", func(t *testing.T) {
			// The view does not exist
			uuid := testOnlineDDLStatementForView(t, createViewStatement, ddlStrategy, "vtgate", "success_create")
			uuids = append(uuids, uuid)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, viewName, true)
			testRevertedUUID(t, uuid, "")
		})
		t.Run("ALTER VIEW, postpone completion", func(t *testing.T) {
			// Technically this test better fits in `onlineddl_scheduler_test.go`, but since we've already laid the grounds here, this is where it landed.
			// The view exists
			checkTable(t, viewName, true)
			uuid := testOnlineDDLStatementForView(t, alterViewStatement, ddlStrategy+" --postpone-completion", "vtgate", "success_create")
			uuids = append(uuids, uuid)

			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
			// Issue a complete and wait for successful completion
			onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
			// This part may take a while, because we depend on vreplication polling
			status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 60*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
			fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
			checkTable(t, viewName, true)
			testRevertedUUID(t, uuid, "")
		})
		// now verify that the revert for ALTER VIEW respects `--postpone-completion`
		testPostponedRevert(t, schema.OnlineDDLStatusQueued, schema.OnlineDDLStatusReady)
		checkTable(t, viewName, true)
	})

	// INSTANT DDL
	t.Run("INSTANT DDL: add column", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, "alter table stress_test add column i_instant int not null default 0", ddlStrategy+" --prefer-instant-ddl", "vtgate", "i_instant")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)

		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		row := rs.Named().Row()
		require.NotNil(t, row)
		specialPlan := row.AsString("special_plan", "")
		artifacts := row.AsString("artifacts", "")
		instantDDLCapable, err := capableOf(mysql.InstantDDLFlavorCapability)
		assert.NoError(t, err)
		if instantDDLCapable {
			// instant DDL expected to apply in 8.0
			assert.Contains(t, specialPlan, "instant-ddl")
			assert.Empty(t, artifacts)
		} else {
			// instant DDL not possible, this is a normal vrepl migration
			assert.Empty(t, specialPlan)
			assert.NotEmpty(t, artifacts)
		}
	})
	t.Run("INSTANT DDL: fail revert", func(t *testing.T) {
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		instantDDLCapable, err := capableOf(mysql.InstantDDLFlavorCapability)
		assert.NoError(t, err)
		if instantDDLCapable {
			// instant DDL expected to apply in 8.0, therefore revert is impossible
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		} else {
			onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		}
	})

	// DROP
	t.Run("online DROP TABLE", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, dropStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE", func(t *testing.T) {
		// This will recreate the table (well, actually, rename it back into place)
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("revert revert DROP TABLE", func(t *testing.T) {
		// This will reapply DROP TABLE
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})

	// DROP IF EXISTS
	t.Run("online DROP TABLE IF EXISTS", func(t *testing.T) {
		// The table doesn't actually exist right now
		uuid := testOnlineDDLStatementForTable(t, dropIfExistsStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE IF EXISTS", func(t *testing.T) {
		// Table will not be recreated because it didn't exist during the DROP TABLE IF EXISTS
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert revert DROP TABLE IF EXISTS", func(t *testing.T) {
		// Table still does not exist
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("revert revert revert DROP TABLE IF EXISTS", func(t *testing.T) {
		// Table still does not exist
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})

	// PARTITIONS
	checkPartitionedTableCountRows := func(t *testing.T, expectRows int64) {
		rs := onlineddl.VtgateExecQuery(t, &vtParams, "select count(*) as c from part_test", "")
		require.NotNil(t, rs)
		row := rs.Named().Row()
		require.NotNil(t, row)
		count, err := row.ToInt64("c")
		require.NoError(t, err)
		assert.Equal(t, expectRows, count)
	}
	t.Run("partitions: create partitioned table", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, createPartitionedTableStatement, ddlStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, partitionedTableName, true)

		populatePartitionedTable(t)
		checkPartitionedTableCountRows(t, 6)
	})
	t.Run("partitions: drop first partition", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, "alter table part_test drop partition `p1`", ddlStrategy+" --fast-range-rotation", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, partitionedTableName, true)

		checkPartitionedTableCountRows(t, 5)
	})
	t.Run("partitions: fail revert drop first partition", func(t *testing.T) {
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)

		checkPartitionedTableCountRows(t, 5)
	})
	t.Run("partitions: add new partition", func(t *testing.T) {
		uuid := testOnlineDDLStatementForTable(t, "alter table part_test add partition (PARTITION p7 VALUES LESS THAN (70))", ddlStrategy+" --fast-range-rotation", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, partitionedTableName, true)
	})
	t.Run("partitions: fail revert add new partition", func(t *testing.T) {
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
	})

	// FAILURES
	t.Run("fail online DROP TABLE", func(t *testing.T) {
		// The table does not exist now
		uuid := testOnlineDDLStatementForTable(t, dropStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkTable(t, tableName, false)
	})
	t.Run("fail revert failed online DROP TABLE", func(t *testing.T) {
		// Cannot revert a failed migration
		uuid := testRevertMigration(t, uuids[len(uuids)-1], ddlStrategy)
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkTable(t, tableName, false)
	})
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, tableName string, expectHint string) (uuid string) {
	if executeStrategy == "vtgate" {
		row := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, alterStatement, "").Named().Row()
		if row != nil {
			uuid = row.AsString("uuid", "")
		}
	} else {
		var err error
		uuid, err = clusterInstance.VtctlclientProcess.ApplySchemaWithOutput(keyspaceName, alterStatement, cluster.VtctlClientParams{DDLStrategy: ddlStrategy})
		assert.NoError(t, err)
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	if !strategySetting.Strategy.IsDirect() {
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

func testOnlineDDLStatementForTable(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, expectHint string) (uuid string) {
	return testOnlineDDLStatement(t, alterStatement, ddlStrategy, executeStrategy, tableName, expectHint)
}

func testOnlineDDLStatementForView(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, expectHint string) (uuid string) {
	return testOnlineDDLStatement(t, alterStatement, ddlStrategy, executeStrategy, viewName, expectHint)
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, revertUUID string, ddlStrategy string) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", revertUUID)
	r := onlineddl.VtgateExecDDL(t, &vtParams, ddlStrategy, revertQuery, "")

	row := r.Named().Row()
	require.NotNil(t, row)

	uuid = row["uuid"].ToString()

	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	_ = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
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
	assert.GreaterOrEqual(t, len(queryResult.Rows[0]), 2) // table name, create statement (for view, also more columns)
	statement = queryResult.Rows[0][1].ToString()
	return statement
}

func generateInsert(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, id)
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
	return err
}

func generateUpdate(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, id)
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
	return err
}

func generateDelete(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
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
	return err
}

func runSingleConnection(ctx context.Context, t *testing.T, done *int64) {
	log.Infof("Running single connection")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	require.Nil(t, err)

	for {
		if atomic.LoadInt64(done) == 1 {
			log.Infof("Terminating single connection")
			return
		}
		switch rand.Int31n(3) {
		case 0:
			err = generateInsert(t, conn)
		case 1:
			err = generateUpdate(t, conn)
		case 2:
			err = generateDelete(t, conn)
		}
		assert.Nil(t, err)
		time.Sleep(10 * time.Millisecond)
	}
}

func runMultipleConnections(ctx context.Context, t *testing.T) {
	log.Infof("Running multiple connections")

	require.True(t, checkTable(t, tableName, true))
	var done int64
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, &done)
		}()
	}
	<-ctx.Done()
	atomic.StoreInt64(&done, 1)
	log.Infof("Running multiple connections: done")
	wg.Wait()
	log.Infof("All connections cancelled")
}

func initTable(t *testing.T) {
	log.Infof("initTable begin")
	defer log.Infof("initTable complete")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	writeMetrics.Clear()
	_, err = conn.ExecuteFetch(truncateStatement, 1000, true)
	require.Nil(t, err)

	for i := 0; i < maxTableRows/2; i++ {
		generateInsert(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateUpdate(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateDelete(t, conn)
	}
}

func testSelectTableMetrics(t *testing.T) {
	writeMetrics.mu.Lock()
	defer writeMetrics.mu.Unlock()

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
