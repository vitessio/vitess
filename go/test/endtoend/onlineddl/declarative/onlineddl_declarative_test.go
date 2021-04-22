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
	vtParams        mysql.ConnParams

	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	tableName             = `stress_test`
	createStatement1      = `
		CREATE TABLE stress_test (
			id bigint(20) not null,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default 'create1',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			key created_idx(created_timestamp),
			key updates_idx(updates)
		) ENGINE=InnoDB
	`
	createStatement2 = `
		CREATE TABLE stress_test (
			id bigint(20) not null,
			rand_val varchar(32) null default '',
			hint_col varchar(64) not null default 'create2',
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
	alterStatement = `
		ALTER TABLE stress_test modify hint_col varchar(64) not null default 'this-should-fail'
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
	maxTableRows = 4096
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
			"-migration_check_interval", "5s",
		}
		clusterInstance.VtGateExtraArgs = []string{
			"-ddl_strategy", "online",
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
	shards := clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))

	declarativeStrategy := "online -declarative"
	var uuids []string

	// CREATE1
	t.Run("declarative CREATE TABLE where table does not exist", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatement(t, createStatement1, declarativeStrategy, "vtgate", "create1")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		initTable(t)
		testSelectTableMetrics(t)
	})
	// CREATE1 again, noop
	t.Run("declarative CREATE TABLE with no changes where table exists", func(t *testing.T) {
		// The exists with exact same schema
		uuid := testOnlineDDLStatement(t, createStatement1, declarativeStrategy, "vtgate", "create1")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, false)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("revert CREATE TABLE expecting noop", func(t *testing.T) {
		// Reverting a noop changes nothing
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkMigratedTable(t, tableName, "create1")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("declarative DROP TABLE", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, dropStatement, declarativeStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, false)
	})
	// Table dropped. Let's start afresh.

	// CREATE1
	t.Run("declarative CREATE TABLE where table does not exist", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatement(t, createStatement1, declarativeStrategy, "vtgate", "create1")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		initTable(t)
		testSelectTableMetrics(t)
	})
	// CREATE2: Change schema
	t.Run("declarative CREATE TABLE with changes where table exists", func(t *testing.T) {
		// The table exists with different schema
		uuid := testOnlineDDLStatement(t, createStatement2, declarativeStrategy, "vtgate", "create2")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("revert CREATE TABLE expecting previous schema", func(t *testing.T) {
		// Reverting back to 1st version
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkMigratedTable(t, tableName, "create1")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("declarative DROP TABLE", func(t *testing.T) {
		// Table exists
		uuid := testOnlineDDLStatement(t, dropStatement, declarativeStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE", func(t *testing.T) {
		// This will recreate the table (well, actually, rename it back into place)
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, true)
		checkMigratedTable(t, tableName, "create1")
		testSelectTableMetrics(t)
	})
	t.Run("revert revert DROP TABLE", func(t *testing.T) {
		// This will reapply DROP TABLE
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	t.Run("declarative DROP TABLE where table does not exist", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, dropStatement, declarativeStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, false)
		checkTable(t, tableName, false)
	})
	t.Run("revert DROP TABLE where table did not exist", func(t *testing.T) {
		// Table will not be recreated because it didn't exist during the previous DROP TABLE
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkTable(t, tableName, false)
	})
	// Table dropped. Let's start afresh.

	// CREATE1
	t.Run("declarative CREATE TABLE where table does not exist", func(t *testing.T) {
		// The table does not exist
		uuid := testOnlineDDLStatement(t, createStatement1, declarativeStrategy, "vtgate", "create1")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		initTable(t)
		testSelectTableMetrics(t)
	})
	// CREATE2
	t.Run("declarative CREATE TABLE with changes where table exists", func(t *testing.T) {
		// The exists but with different schema
		uuid := testOnlineDDLStatement(t, createStatement2, declarativeStrategy, "vtgate", "create2")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	// CREATE1 again
	t.Run("declarative CREATE TABLE again with changes where table exists", func(t *testing.T) {
		// The exists but with different schema
		uuid := testOnlineDDLStatement(t, createStatement1, declarativeStrategy, "vtgate", "create1")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		onlineddl.CheckMigrationArtifacts(t, &vtParams, shards, uuid, true)
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("revert CREATE TABLE expecting previous schema", func(t *testing.T) {
		// Reverting back to previous version
		uuid := testRevertMigration(t, uuids[len(uuids)-1])
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		checkMigratedTable(t, tableName, "create2")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("ALTER TABLE expecting failure", func(t *testing.T) {
		// ALTER is not supported in -declarative
		uuid := testOnlineDDLStatement(t, alterStatement, declarativeStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkMigratedTable(t, tableName, "create2")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("CREATE TABLE IF NOT EXISTS expecting failure", func(t *testing.T) {
		// IF NOT EXISTS is not supported in -declarative
		uuid := testOnlineDDLStatement(t, createIfNotExistsStatement, declarativeStrategy, "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusFailed)
		checkMigratedTable(t, tableName, "create2")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
	t.Run("CREATE TABLE IF NOT EXISTS non-declarative is successful", func(t *testing.T) {
		// IF NOT EXISTS is supported in non-declarative mode. Just verifying that the statement itself is good,
		// so that the failure we tested for, above, actually tests the "declarative" logic, rather than some
		// unrelated error.
		uuid := testOnlineDDLStatement(t, createIfNotExistsStatement, "online", "vtgate", "")
		uuids = append(uuids, uuid)
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		// the table existed, so we expect no changes in this non-declarative DDL
		checkMigratedTable(t, tableName, "create2")
		checkTable(t, tableName, true)
		testSelectTableMetrics(t)
	})
}

// testOnlineDDLStatement runs an online DDL, ALTER statement
func testOnlineDDLStatement(t *testing.T, alterStatement string, ddlStrategy string, executeStrategy string, expectHint string) (uuid string) {
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
		time.Sleep(time.Second * 20)
	}

	if expectHint != "" {
		checkMigratedTable(t, tableName, expectHint)
	}
	return uuid
}

// testRevertMigration reverts a given migration
func testRevertMigration(t *testing.T, revertUUID string) (uuid string) {
	revertQuery := fmt.Sprintf("revert vitess_migration '%s'", revertUUID)
	r := onlineddl.VtgateExecQuery(t, &vtParams, revertQuery, "")

	row := r.Named().Row()
	require.NotNil(t, row)

	uuid = row["uuid"].ToString()

	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	time.Sleep(time.Second * 20)
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
	require.Nil(t, err)
	defer conn.Close()

	rs, err := conn.ExecuteFetch(selectCountRowsStatement, 1000, true)
	require.Nil(t, err)

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
