/*
Copyright 2023 The Vitess Authors.

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

package fkstress

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// This endtoend test is designd to validate VTGate's FOREIGN KEY implementation for unsharded/single-sharded/shard-scope, meaning
// we expect foreign key constraints to be limited to a shard (related rows can never be on diffrent shards).
//
// This test validates NO ACTION, CASCADE and SET NULL reference actions.
// VTGate's support for foreign keys includes:
// - Analyzing the foreign key constraints in a keyspace.
// - Rejecting INSERT statements for child table when there's no matching row on a parent table.
// - Handling DELETE and UPDATE statements on a parent table according to the reference action on all children.
//   Specifically, this means for example that VTGate will handle a ON DELETE CASCADE in Vitess plane. It will first delete rows
//   from the child (recursive operation) before deleting the row on the parent. As result, the underlying MySQL server will have
//   nothing to cascade.
//
// The design of this test is as follows:
// - Create a cluster with PRIMARY and REPLICA tablets
// - Given this structure of tables with foreign key constraints:
//   stress_parent
//   +- stress_child
//      +- stress_grandchild
//   +- stress_child2
// - Create these tables. Then, on the MySQL replica, remove the foreign key constraints.
// - Static test:
//   - Randomly populate all tables via highly-contentive INSERT/UPDATE/DELETE statements
//   - Validate collected metrics match actual table data
//   - Validate foreign key constraints integrity
// - Workload test:
//   - Initially populate tables as above
//   - Run a high contention workload where multiple connections issue random INSERT/UPDATE/DELETE on all related tables
//   - Validate collected metrics match actual table data
//   - Validate foreign key constraints integrity on MySQL primary
//   - Validate foreign key constraints integrity on MySQL replica
//   - Compare data on primary & replica
//
// We of course know that foreign key integrity is maintained on the MySQL primary. However, the replica does not have the matching
// constraints. Since cascaded (SET NULL, CASCADE) writes are handled internally by InnoDB and not written to the binary log,
// any cascaded writes on the primary are lost, and the replica is unaware of those writes. Without VTGate intervention, we expect
// the replica to quickly diverge from the primary, and in fact in all likelyhood replication will break very quickly.
// However, if VTGate implements the cascading rules correctly, the primary MySQL server will never have any actual cascades, and
// so cascaded writes are all accounted for in the binary logs, which means we can expect the replica to be compliant with the
// primary.

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
	primary         *cluster.Vttablet
	replica         *cluster.Vttablet
	vtParams        mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	parentTableName       = "stress_parent"
	childTableName        = "stress_child"
	child2TableName       = "stress_child2"
	grandchildTableName   = "stress_grandchild"
	tableNames            = []string{parentTableName, childTableName, child2TableName, grandchildTableName}
	reverseTableNames     []string

	referenceActionMap = map[sqlparser.ReferenceAction]string{
		sqlparser.NoAction: "NO ACTION",
		sqlparser.Cascade:  "CASCADE",
		sqlparser.SetNull:  "SET NULL",
	}
	referenceActions = []sqlparser.ReferenceAction{sqlparser.NoAction, sqlparser.Cascade, sqlparser.SetNull}
	createStatements = []string{
		`
		CREATE TABLE stress_parent (
			id bigint not null,
			parent_id bigint,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key parent_id_idx(parent_id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
		`,
		`
		CREATE TABLE stress_child (
			id bigint not null,
			parent_id bigint,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key parent_id_idx(parent_id),
			key created_idx(created_timestamp),
			key updates_idx(updates),
			CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES stress_parent (id) ON DELETE %s
		) ENGINE=InnoDB
		`,
		`
		CREATE TABLE stress_child2 (
			id bigint not null,
			parent_id bigint,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key parent_id_idx(parent_id),
			key created_idx(created_timestamp),
			key updates_idx(updates),
			CONSTRAINT child2_parent_fk FOREIGN KEY (parent_id) REFERENCES stress_parent (id) ON DELETE %s
		) ENGINE=InnoDB
		`,
		`
		CREATE TABLE stress_grandchild (
			id bigint not null,
			parent_id bigint,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key parent_id_idx(parent_id),
			key created_idx(created_timestamp),
			key updates_idx(updates),
			CONSTRAINT grandchild_child_fk FOREIGN KEY (parent_id) REFERENCES stress_child (id) ON DELETE %s
		) ENGINE=InnoDB
		`,
	}
	dropConstraintsStatements = []string{
		`ALTER TABLE stress_child DROP CONSTRAINT child_parent_fk`,
		`ALTER TABLE stress_child2 DROP CONSTRAINT child2_parent_fk`,
		`ALTER TABLE stress_grandchild DROP CONSTRAINT grandchild_child_fk`,
	}
	insertRowStatement = `
		INSERT IGNORE INTO %s (id, parent_id, rand_val) VALUES (%d, %d, left(md5(rand()), 8))
	`
	updateRowStatement = `
		UPDATE %s SET rand_val=left(md5(rand()), 8), updates=updates+1 WHERE id=%d
	`
	updateRowIdStatement = `
		UPDATE %s SET id=%v, rand_val=left(md5(rand()), 8), updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM %s WHERE id=%d AND updates=1
	`
	// We use CAST(SUM(updates) AS SIGNED) because SUM() returns a DECIMAL datatype, and we want to read a SIGNED INTEGER type
	selectCountRowsStatement = `
		SELECT COUNT(*) AS num_rows, CAST(SUM(updates) AS SIGNED) AS sum_updates FROM %s
	`
	selectMatchingRowsChild = `
		select stress_child.id from stress_child join stress_parent on (stress_parent.id = stress_child.parent_id)
	`
	selectMatchingRowsChild2 = `
		select stress_child2.id from stress_child2 join stress_parent on (stress_parent.id = stress_child2.parent_id)
	`
	selectMatchingRowsGrandchild = `
		select stress_grandchild.id from stress_grandchild join stress_child on (stress_child.id = stress_grandchild.parent_id)
	`
	selectOrphanedRowsChild = `
		select stress_child.id from stress_child left join stress_parent on (stress_parent.id = stress_child.parent_id) where stress_parent.id is null
	`
	selectOrphanedRowsChild2 = `
		select stress_child2.id from stress_child2 left join stress_parent on (stress_parent.id = stress_child2.parent_id) where stress_parent.id is null
	`
	selectOrphanedRowsGrandchild = `
		select stress_grandchild.id from stress_grandchild left join stress_child on (stress_child.id = stress_grandchild.parent_id) where stress_child.id is null
	`
	deleteAllStatement = `
		DELETE FROM %s
	`
	writeMetrics = map[string]*WriteMetrics{}
)

const (
	maxTableRows = 4096
)

// The following variables are fit for a local, strong developer box.
// The test overrides these into more relaxed values if running on GITHUB_ACTIONS,
// seeing that GitHub CI is much weaker.
var (
	maxConcurrency                = 10
	singleConnectionSleepInterval = 10 * time.Millisecond
	countIterations               = 3
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

		// We will use a replica to confirm that vtgate's cascading works correctly.
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
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

func queryTablet(t *testing.T, tablet *cluster.Vttablet, query string, expectError string) *sqltypes.Result {
	rs, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	if expectError == "" {
		assert.NoError(t, err)
	} else {
		assert.ErrorContains(t, err, expectError)
	}
	return rs
}

func tabletTestName(t *testing.T, tablet *cluster.Vttablet) string {
	switch tablet {
	case primary:
		return "primary"
	case replica:
		return "replica"
	default:
		assert.FailNowf(t, "unknown tablet", "%v, type=%v", tablet.Alias, tablet.Type)
	}
	return ""
}

func validateReplicationIsHealthy(t *testing.T, tablet *cluster.Vttablet) bool {
	query := "show replica status"
	rs, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)
	row := rs.Named().Row()
	require.NotNil(t, row)

	ioRunning := row.AsString("Replica_IO_Running", "")
	require.NotEmpty(t, ioRunning)
	ioHealthy := assert.Equalf(t, "Yes", ioRunning, "Replication is broken. Replication status: %v", row)
	sqlRunning := row.AsString("Replica_SQL_Running", "")
	require.NotEmpty(t, sqlRunning)
	sqlHealthy := assert.Equalf(t, "Yes", sqlRunning, "Replication is broken. Replication status: %v", row)

	return ioHealthy && sqlHealthy
}

func getTabletPosition(t *testing.T, tablet *cluster.Vttablet) replication.Position {
	rs := queryTablet(t, tablet, "select @@gtid_executed as gtid_executed", "")
	row := rs.Named().Row()
	require.NotNil(t, row)
	gtidExecuted := row.AsString("gtid_executed", "")
	require.NotEmpty(t, gtidExecuted)
	pos, err := replication.DecodePositionDefaultFlavor(gtidExecuted, replication.Mysql56FlavorID)
	assert.NoError(t, err)
	return pos
}

func waitForReplicaCatchup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	primaryPos := getTabletPosition(t, primary)
	for {
		replicaPos := getTabletPosition(t, replica)
		if replicaPos.GTIDSet.Contains(primaryPos.GTIDSet) {
			// success
			return
		}
		if !validateReplicationIsHealthy(t, replica) {
			assert.FailNow(t, "replication is broken; not waiting for catchup")
			return
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "timeout waiting for replica to catch up")
			return
		case <-time.After(time.Second):
			//
		}
	}
}

func TestStressFK(t *testing.T) {
	defer cluster.PanicHandler(t)

	if val, present := os.LookupEnv("GITHUB_ACTIONS"); present && val != "" {
		// This is the place to fine tune the stress parameters if GitHub actions are too slow
		maxConcurrency = maxConcurrency * 1
		singleConnectionSleepInterval = singleConnectionSleepInterval * 1
	}
	t.Logf("==== test setup: maxConcurrency=%v, singleConnectionSleepInterval=%v", maxConcurrency, singleConnectionSleepInterval)

	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))
	require.Equal(t, 2, len(shards[0].Vttablets))
	primary = shards[0].Vttablets[0]
	require.NotNil(t, primary)
	replica = shards[0].Vttablets[1]
	require.NotNil(t, replica)
	require.NotEqual(t, primary.Alias, replica.Alias)

	tableNames = []string{parentTableName, childTableName, child2TableName, grandchildTableName}
	reverseTableNames = slices.Clone(tableNames)
	slices.Reverse(reverseTableNames)
	require.ElementsMatch(t, tableNames, reverseTableNames)

	for _, tableName := range tableNames {
		writeMetrics[tableName] = &WriteMetrics{}
	}

	t.Run("validate replication health", func(t *testing.T) {
		validateReplicationIsHealthy(t, replica)
	})

	validateMetrics := func(t *testing.T, onDeleteAction sqlparser.ReferenceAction) {
		for _, workloadTable := range []string{parentTableName, childTableName, child2TableName, grandchildTableName} {
			tname := fmt.Sprintf("validate metrics: %s", workloadTable)
			t.Run(tname, func(t *testing.T) {
				var primaryRows, replicaRows int64
				t.Run(tabletTestName(t, primary), func(t *testing.T) {
					primaryRows = testSelectTableMetrics(t, primary, workloadTable, onDeleteAction)
				})
				t.Run(tabletTestName(t, replica), func(t *testing.T) {
					replicaRows = testSelectTableMetrics(t, replica, workloadTable, onDeleteAction)
				})
				t.Run("compare primary and replica", func(t *testing.T) {
					assert.Equal(t, primaryRows, replicaRows)
				})
			})
		}
	}

	t.Run("static data", func(t *testing.T) {
		for _, onDeleteAction := range referenceActions {
			t.Run(referenceActionMap[onDeleteAction], func(t *testing.T) {
				t.Run("create schema", func(t *testing.T) {
					createInitialSchema(t, onDeleteAction)
				})
				t.Run("init tables", func(t *testing.T) {
					populateTables(t)
				})
				t.Run("wait for replica", func(t *testing.T) {
					waitForReplicaCatchup(t)
				})
				t.Run("validate metrics", func(t *testing.T) {
					validateMetrics(t, onDeleteAction)
				})
				t.Run("validate replication health", func(t *testing.T) {
					validateReplicationIsHealthy(t, replica)
				})
			})
		}
	})
	t.Run("stress", func(t *testing.T) {
		for _, onDeleteAction := range referenceActions {
			t.Run(referenceActionMap[onDeleteAction], func(t *testing.T) {
				// This tests running a workload on the table, then comparing expected metrics with
				// actual table metrics. All this without any ALTER TABLE: this is to validate
				// that our testing/metrics logic is sound in the first place.
				t.Run("Workload", func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					t.Run("create schema", func(t *testing.T) {
						createInitialSchema(t, onDeleteAction)
					})
					t.Run("init tables", func(t *testing.T) {
						populateTables(t)
					})
					t.Run("workload", func(t *testing.T) {
						var wg sync.WaitGroup
						for _, workloadTable := range []string{parentTableName, childTableName, child2TableName, grandchildTableName} {
							wg.Add(1)
							go func(tbl string) {
								defer wg.Done()
								runMultipleConnections(ctx, t, tbl)
							}(workloadTable)
						}
						time.Sleep(5 * time.Second)
						cancel() // will cause runMultipleConnections() to terminate
						wg.Wait()
					})
					t.Run("wait for replica", func(t *testing.T) {
						waitForReplicaCatchup(t)
					})
					validateMetrics(t, onDeleteAction)
					t.Run("validate replication health", func(t *testing.T) {
						validateReplicationIsHealthy(t, replica)
					})
					t.Run("validate fk", func(t *testing.T) {
						testFKIntegrity(t, primary, onDeleteAction)
						testFKIntegrity(t, replica, onDeleteAction)
					})
				})
			})
		}
	})
}

// createInitialSchema creates the tables from scratch, and drops the foreign key constraints on the replica.
func createInitialSchema(t *testing.T, onDeleteAction sqlparser.ReferenceAction) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	t.Run("dropping tables", func(t *testing.T) {
		for _, tableName := range reverseTableNames {
			err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, "drop table if exists "+tableName)
			require.NoError(t, err)
		}
	})
	t.Run("creating tables", func(t *testing.T) {
		// Create the stress tables
		var b strings.Builder
		for _, sql := range createStatements {
			b.WriteString(fmt.Sprintf(sql, referenceActionMap[onDeleteAction]))
			b.WriteString(";")
		}
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, b.String())
		require.NoError(t, err)

		rs, err := conn.ExecuteFetch("show full tables", 1000, true)
		require.NoError(t, err)
		require.Equal(t, 4, len(rs.Rows))
		t.Logf("===== init: %d tables created", len(rs.Rows))
	})
	t.Run("wait for replica", func(t *testing.T) {
		waitForReplicaCatchup(t)
	})
	t.Run("validating tables: vttablet", func(t *testing.T) {
		// Check if table is created. Checked on tablets.
		checkTable(t, parentTableName)
		checkTable(t, childTableName)
		checkTable(t, child2TableName)
		checkTable(t, grandchildTableName)
	})
	t.Run("validating tables: vtgate", func(t *testing.T) {
		// Wait for tables to appear on VTGate
		waitForTable(t, parentTableName, conn)
		waitForTable(t, childTableName, conn)
		waitForTable(t, child2TableName, conn)
		waitForTable(t, grandchildTableName, conn)
	})
	t.Run("dropping foreign keys on replica", func(t *testing.T) {
		for _, statement := range dropConstraintsStatements {
			_ = queryTablet(t, replica, "set global super_read_only=0", "")
			_ = queryTablet(t, replica, statement, "")
			_ = queryTablet(t, replica, "set global super_read_only=1", "")
		}
	})
	t.Run("validate definitions", func(t *testing.T) {
		for _, tableName := range []string{childTableName, child2TableName, grandchildTableName} {
			t.Run(tableName, func(t *testing.T) {
				t.Run(tabletTestName(t, primary), func(t *testing.T) {
					stmt := getCreateTableStatement(t, primary, tableName)
					assert.Contains(t, stmt, "CONSTRAINT")
				})
				t.Run(tabletTestName(t, replica), func(t *testing.T) {
					stmt := getCreateTableStatement(t, replica, tableName)
					assert.NotContains(t, stmt, "CONSTRAINT")
				})
			})
		}
	})
}

// waitForTable waits until table is seen in VTGate
func waitForTable(t *testing.T, tableName string, conn *mysql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	query := fmt.Sprintf("select count(*) from %s", tableName)
	for {
		if _, err := conn.ExecuteFetch(query, 1, false); err == nil {
			return // good
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fail()
			return
		}
	}
}

// checkTable checks that the given table exists on all tablets
func checkTable(t *testing.T, showTableName string) {
	for _, tablet := range shards[0].Vttablets {
		checkTablesCount(t, tablet, showTableName, 1)
	}
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, showTableName string, expectCount int) {
	query := fmt.Sprintf(`show tables like '%s';`, showTableName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rowcount := 0
	for {
		queryResult, err := tablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
		require.Nil(t, err)
		rowcount = len(queryResult.Rows)
		if rowcount > 0 {
			break
		}

		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			break
		}
	}
	assert.Equal(t, expectCount, rowcount)
}

// getCreateTableStatement returns the CREATE TABLE statement for a given table
func getCreateTableStatement(t *testing.T, tablet *cluster.Vttablet, tableName string) (statement string) {
	queryResult := queryTablet(t, tablet, fmt.Sprintf("show create table %s", tableName), "")

	require.Equal(t, len(queryResult.Rows), 1)
	row := queryResult.Rows[0]
	assert.Equal(t, len(row), 2) // table name, create statement
	statement = row[1].ToString()
	return statement
}

func generateInsert(t *testing.T, tableName string, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	parentId := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, tableName, id, parentId)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].insertsAttempts++
		if err != nil {
			writeMetrics[tableName].insertsFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics[tableName].insertsNoops++
			return
		}
		writeMetrics[tableName].inserts++
	}()
	return err
}

func generateUpdate(t *testing.T, tableName string, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, tableName, id)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].updatesAttempts++
		if err != nil {
			writeMetrics[tableName].updatesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics[tableName].updatesNoops++
			return
		}
		writeMetrics[tableName].updates++
	}()
	return err
}

func generateDelete(t *testing.T, tableName string, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, tableName, id)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].deletesAttempts++
		if err != nil {
			writeMetrics[tableName].deletesFailures++
			return
		}
		assert.Less(t, qr.RowsAffected, uint64(2))
		if qr.RowsAffected == 0 {
			writeMetrics[tableName].deletesNoops++
			return
		}
		writeMetrics[tableName].deletes++
	}()
	return err
}

func runSingleConnection(ctx context.Context, t *testing.T, tableName string, done *int64) {
	log.Infof("Running single connection on %s", tableName)
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
			_ = generateInsert(t, tableName, conn)
		case 1:
			_ = generateUpdate(t, tableName, conn)
		case 2:
			_ = generateDelete(t, tableName, conn)
		}
		time.Sleep(singleConnectionSleepInterval)
	}
}

func runMultipleConnections(ctx context.Context, t *testing.T, tableName string) {
	log.Infof("Running multiple connections")
	var done int64
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, tableName, &done)
		}()
	}
	<-ctx.Done()
	atomic.StoreInt64(&done, 1)
	log.Infof("Running multiple connections: done")
	wg.Wait()
	log.Infof("All connections cancelled")
}

func wrapWithNoFKChecks(sql string) string {
	return fmt.Sprintf("set foreign_key_checks=0; %s; set foreign_key_checks=1;", sql)
}

// populateTables randomly populates all test tables. This is done sequentially.
func populateTables(t *testing.T) {
	log.Infof("initTable begin")
	defer log.Infof("initTable complete")

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	t.Logf("===== clearing tables")
	for _, tableName := range reverseTableNames {
		writeMetrics[tableName].Clear()
		deleteQuery := fmt.Sprintf(deleteAllStatement, tableName)
		_, err = conn.ExecuteFetch(deleteQuery, 1000, true)
		require.Nil(t, err)
	}
	t.Logf("===== populating tables")
	for _, tableName := range tableNames {
		// populate parent, then child, child2, then grandchild
		for i := 0; i < maxTableRows/2; i++ {
			generateInsert(t, tableName, conn)
		}
		for i := 0; i < maxTableRows/4; i++ {
			generateUpdate(t, tableName, conn)
		}
		for i := 0; i < maxTableRows/4; i++ {
			generateDelete(t, tableName, conn)
		}
	}
}

// testSelectTableMetrics cross references the known metrics (number of successful insert/delete/updates) on each table, with the
// actual number of rows and with the row values on those tables.
// With CASCADE/SET NULL rules we can't do the comparison, because child tables are implicitly affected by the cascading rules,
// and the values do not match what reported to us when we UPDATE/DELETE on the parent tables.
func testSelectTableMetrics(t *testing.T, tablet *cluster.Vttablet, tableName string, onDeleteAction sqlparser.ReferenceAction) int64 {
	switch onDeleteAction {
	case sqlparser.Cascade, sqlparser.SetNull:
		if tableName != parentTableName {
			// We can't validate those tables because they will have been affected by cascading rules.
			return 0
		}
	}
	writeMetrics[tableName].mu.Lock()
	defer writeMetrics[tableName].mu.Unlock()

	log.Infof("%s %s", tableName, writeMetrics[tableName].String())

	rs := queryTablet(t, tablet, fmt.Sprintf(selectCountRowsStatement, tableName), "")

	row := rs.Named().Row()
	require.NotNil(t, row)
	log.Infof("testSelectTableMetrics, row: %v", row)
	numRows := row.AsInt64("num_rows", 0)
	sumUpdates := row.AsInt64("sum_updates", 0)
	assert.NotZero(t, numRows)
	assert.NotZero(t, sumUpdates)
	assert.NotZero(t, writeMetrics[tableName].inserts)
	assert.NotZero(t, writeMetrics[tableName].deletes)
	assert.NotZero(t, writeMetrics[tableName].updates)
	assert.Equal(t, writeMetrics[tableName].inserts-writeMetrics[tableName].deletes, numRows)
	assert.Equal(t, writeMetrics[tableName].updates-writeMetrics[tableName].deletes, sumUpdates) // because we DELETE WHERE updates=1

	return numRows
}

// testFKIntegrity validates that foreign key consitency is maintained on the given tablet. We cross reference all
// parent-child relationships.
// There are two test types:
// 1. Do a JOIN on parent-child associated rows, expect non-empty
// 2. Check that there are no orphaned child rows. Notes:
//   - This applies to NO ACTION and CASCADE, but not to SET NULL, because SET NULL by design creates orphaned rows.
//   - On the primary database, this test trivially passes because of course MySQL maintains this integrity. But remember
//     that we remove the foreign key constraints on the replica. Also remember that cascaded writes are not written to
//     the binary log. And so, if VTGate does not do a proper job, then a parent and child will drift apart in CASCADE writes.
func testFKIntegrity(t *testing.T, tablet *cluster.Vttablet, onDeleteAction sqlparser.ReferenceAction) {
	testName := tabletTestName(t, tablet)
	t.Run(testName, func(t *testing.T) {
		t.Run("matching parent-child rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsChild, "")
			assert.NotZero(t, len(rs.Rows))
			t.Logf("===== matching rows: %v", len(rs.Rows))
		})
		t.Run("matching parent-child2 rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsChild2, "")
			assert.NotZero(t, len(rs.Rows))
			t.Logf("===== matching rows: %v", len(rs.Rows))
		})
		t.Run("matching child-grandchild rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsGrandchild, "")
			assert.NotZero(t, len(rs.Rows))
			t.Logf("===== matching rows: %v", len(rs.Rows))
		})
		if onDeleteAction != sqlparser.SetNull {
			// Because with SET NULL there _are_ orphaned rows
			t.Run("parent-child orphaned rows", func(t *testing.T) {
				rs := queryTablet(t, tablet, selectOrphanedRowsChild, "")
				assert.Zero(t, len(rs.Rows))
			})
			t.Run("parent-child2 orphaned rows", func(t *testing.T) {
				rs := queryTablet(t, tablet, selectOrphanedRowsChild2, "")
				assert.Zero(t, len(rs.Rows))
			})
			t.Run("child-grandchild orphaned rows", func(t *testing.T) {
				rs := queryTablet(t, tablet, selectOrphanedRowsGrandchild, "")
				assert.Zero(t, len(rs.Rows))
			})
		}
	})
}
