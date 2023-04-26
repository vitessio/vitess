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

/*
	This endtoend suite tests VReplication based Online DDL under stress (concurrent INSERT/UPDATE/DELETE queries),
	and looks for before/after table data to be identical.
	This suite specifically targets choice of unique key: PRIMARY vs non-PRIMARY, numeric, textual, compound.
	The scenarios caused by this suite cause VReplication to iterate the table in:
	- expected incremental order (id)
	- expected decremental order (negative id values)
	- random order (random textual checksums)
	- other
*/

package vreplstress

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
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testcase struct {
	// name is a human readable name for the test
	name string
	// prepareStatement modifies the table (direct strategy) before it gets populated
	prepareStatement string
	// alterStatement is the online statement used in the test
	alterStatement string
	// expectFailure is typically false. If true then we do not compare table data results
	expectFailure bool
	// expectAddedUniqueKeys is the number of added constraints
	expectAddedUniqueKeys int64
	// expectRemovedUniqueKeys is the number of alleviated constraints
	expectRemovedUniqueKeys int64
	// autoIncInsert is a special case where we don't generate id values. It's a specific test case.
	autoIncInsert bool
}

var (
	clusterInstance      *cluster.LocalProcessCluster
	vtParams             mysql.ConnParams
	evaluatedMysqlParams *mysql.ConnParams

	directDDLStrategy     = "direct"
	onlineDDLStrategy     = "vitess -vreplication-test-suite -skip-topo"
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	shards                []cluster.Shard
	opOrder               int64
	opOrderMutex          sync.Mutex
	schemaChangeDirectory = ""
	tableName             = "stress_test"
	afterTableName        = "stress_test_after"
	cleanupStatements     = []string{
		`DROP TABLE IF EXISTS stress_test`,
		`DROP TABLE IF EXISTS stress_test_before`,
		`DROP TABLE IF EXISTS stress_test_after`,
	}
	createStatement = `
		CREATE TABLE stress_test (
			id bigint not null,
			id_negative bigint not null,
			rand_text varchar(40) not null default '',
			rand_num bigint unsigned not null,
			nullable_num int default null,
			op_order bigint unsigned not null default 0,
			hint_col varchar(64) not null default '',
			created_timestamp timestamp not null default current_timestamp,
			updates int unsigned not null default 0,
			PRIMARY KEY (id),
			KEY id_idx(id)
		) ENGINE=InnoDB
	`
	testCases = []testcase{
		{
			name:             "trivial PK",
			prepareStatement: "",
			alterStatement:   "engine=innodb",
		},
		{
			name:             "autoinc PK",
			prepareStatement: "modify id bigint not null auto_increment",
			alterStatement:   "engine=innodb",
			autoIncInsert:    true,
		},
		{
			name:             "UK similar to PK, no PK",
			prepareStatement: "add unique key id_uidx(id)",
			alterStatement:   "drop primary key",
		},
		{
			name:             "negative PK",
			prepareStatement: "drop primary key, add primary key (id_negative)",
			alterStatement:   "engine=innodb",
		},
		{
			name:                    "negative UK, no PK",
			prepareStatement:        "add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "negative UK, different PK",
			prepareStatement:        "add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, add primary key(rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "text UK, no PK",
			prepareStatement:        "add unique key text_uidx(rand_text(40))",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "text UK, different PK",
			prepareStatement:        "add unique key text_uidx(rand_text(40))",
			alterStatement:          "drop primary key, add primary key (id, id_negative)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "compound UK 1 by text, no PK",
			prepareStatement:        "add unique key compound_uidx(rand_text(40), id_negative)",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "compound UK 2 by negative, no PK",
			prepareStatement:        "add unique key compound_uidx(id_negative, rand_text(40))",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "compound UK 3 by ascending int, no PK",
			prepareStatement:        "add unique key compound_uidx(id, rand_num, rand_text(40))",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "compound UK 4 by rand int, no PK",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40))",
			alterStatement:          "drop primary key",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "compound UK 5 by rand int, different PK",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40))",
			alterStatement:          "drop primary key, add primary key (id, id_negative)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "multiple UK choices 1",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40)), add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, add primary key(updates, id)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "multiple UK choices 2",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40)), add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, add primary key(id, id_negative)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "multiple UK choices including nullable with PK",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40)), add unique key nullable_uidx(nullable_num, id_negative), add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, drop key negative_uidx, add primary key(id_negative)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "multiple UK choices including nullable",
			prepareStatement:        "add unique key compound_uidx(rand_num, rand_text(40)), add unique key nullable_uidx(nullable_num, id_negative), add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, add primary key(updates, id)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add primary key(id_negative)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "extended PRIMARY KEY",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add primary key(id, id_negative)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "extended PRIMARY KEY, different order",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add primary key(id_negative, id)",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                  "reduced PRIMARY KEY",
			prepareStatement:      "drop primary key, add primary key(id, id_negative)",
			alterStatement:        "drop primary key, add primary key(id)",
			expectAddedUniqueKeys: 1,
		},
		{
			name:                  "reduced PRIMARY KEY 2",
			prepareStatement:      "drop primary key, add primary key(id, id_negative)",
			alterStatement:        "drop primary key, add primary key(id_negative)",
			expectAddedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY, text",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add primary key(rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY, rand",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add primary key(rand_num, rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY, from negative to int",
			prepareStatement:        "drop primary key, add primary key(id_negative)",
			alterStatement:          "drop primary key, add primary key(id)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY, from text to int",
			prepareStatement:        "drop primary key, add primary key(rand_text(40))",
			alterStatement:          "drop primary key, add primary key(id)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY, from text to rand",
			prepareStatement:        "drop primary key, add primary key(rand_text(40))",
			alterStatement:          "drop primary key, add primary key(rand_num, rand_text(40))",
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "partially shared PRIMARY KEY 1",
			prepareStatement:        "drop primary key, add primary key(id, id_negative)",
			alterStatement:          "drop primary key, add primary key(id, rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "partially shared PRIMARY KEY 2",
			prepareStatement:        "drop primary key, add primary key(id, id_negative)",
			alterStatement:          "drop primary key, add primary key(id_negative, rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "partially shared PRIMARY KEY 3",
			prepareStatement:        "drop primary key, add primary key(id, id_negative)",
			alterStatement:          "drop primary key, add primary key(rand_text(40), id)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "partially shared PRIMARY KEY 4",
			prepareStatement:        "drop primary key, add primary key(id_negative, id)",
			alterStatement:          "drop primary key, add primary key(rand_text(40), id)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "different PRIMARY KEY vs UNIQUE KEY",
			prepareStatement:        "",
			alterStatement:          "drop primary key, add unique key(id_negative)",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 1,
		},
		{
			name:                    "no shared UK, multiple options",
			prepareStatement:        "add unique key negative_uidx(id_negative)",
			alterStatement:          "drop primary key, drop key negative_uidx, add primary key(rand_text(40)), add unique key negtext_uidx(id_negative, rand_text(40))",
			expectAddedUniqueKeys:   1,
			expectRemovedUniqueKeys: 2,
		},
		{
			name:             "fail; no uk on target",
			prepareStatement: "",
			alterStatement:   "drop primary key",
			expectFailure:    true,
		},
		{
			name:             "fail; only nullable shared uk",
			prepareStatement: "add unique key nullable_uidx(nullable_num)",
			alterStatement:   "drop primary key",
			expectFailure:    true,
		},
	}
	alterHintStatement = `
		alter table stress_test modify hint_col varchar(64) not null default '%s'
	`

	insertRowAutoIncStatement = `
		INSERT IGNORE INTO stress_test (id, id_negative, rand_text, rand_num, op_order) VALUES (NULL, %d, concat(left(md5(%d), 8), '_', %d), floor(rand()*1000000), %d)
	`
	insertRowStatement = `
		INSERT IGNORE INTO stress_test (id, id_negative, rand_text, rand_num, op_order) VALUES (%d, %d, concat(left(md5(%d), 8), '_', %d), floor(rand()*1000000), %d)
	`
	updateRowStatement = `
		UPDATE stress_test SET op_order=%d, updates=updates+1 WHERE id=%d
	`
	deleteRowStatement = `
		DELETE FROM stress_test WHERE id=%d
	`
	selectCountFromTable = `
		SELECT count(*) as c FROM stress_test
	`
	selectCountFromTableBefore = `
		SELECT count(*) as c FROM stress_test_before
	`
	selectCountFromTableAfter = `
		SELECT count(*) as c FROM stress_test_after
	`
	selectMaxOpOrderFromTableBefore = `
		SELECT MAX(op_order) as m FROM stress_test_before
	`
	selectMaxOpOrderFromTableAfter = `
		SELECT MAX(op_order) as m FROM stress_test_after
	`
	selectBeforeTable = `
		SELECT * FROM stress_test_before order by id, id_negative, rand_text, rand_num
	`
	selectAfterTable = `
		SELECT * FROM stress_test_after order by id, id_negative, rand_text, rand_num
	`
	truncateStatement = `
		TRUNCATE TABLE stress_test
	`
)

const (
	maxTableRows                  = 4096
	maxConcurrency                = 15
	singleConnectionSleepInterval = 5 * time.Millisecond
	periodicSleepPercent          = 10 // in the range (0,100). 10 means 10% sleep time throught the stress load.
	waitForStatusTimeout          = 180 * time.Second
)

func resetOpOrder() {
	opOrderMutex.Lock()
	defer opOrderMutex.Unlock()
	opOrder = 0
}

func nextOpOrder() int64 {
	opOrderMutex.Lock()
	defer opOrderMutex.Unlock()
	opOrder++
	return opOrder
}

func getTablet() *cluster.Vttablet {
	return clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
}

func mysqlParams() *mysql.ConnParams {
	if evaluatedMysqlParams != nil {
		return evaluatedMysqlParams
	}
	evaluatedMysqlParams = &mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", getTablet().TabletUID), "/mysql.sock"),
		DbName:     fmt.Sprintf("vt_%s", keyspaceName),
	}
	return evaluatedMysqlParams
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
			"--schema_change_check_interval", "1",
		}

		// --vstream_packet_size is set to a small value that ensures we get multiple stream iterations,
		// thereby examining lastPK on vcopier side. We will be iterating tables using non-PK order throughout
		// this test suite, and so the low setting ensures we hit the more interesting code paths.
		clusterInstance.VtTabletExtraArgs = []string{
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", "5s",
			"--migration_check_interval", "5s",
			"--vstream_packet_size", "4096", // Keep this value small and below 10k to ensure multilple vstream iterations
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

	for _, testcase := range testCases {
		require.NotEmpty(t, testcase.name)
		t.Run(testcase.name, func(t *testing.T) {
			t.Run("cancel pending migrations", func(t *testing.T) {
				cancelQuery := "alter vitess_migration cancel all"
				r := onlineddl.VtgateExecQuery(t, &vtParams, cancelQuery, "")
				if r.RowsAffected > 0 {
					fmt.Printf("# Cancelled migrations (for debug purposes): %d\n", r.RowsAffected)
				}
			})
			t.Run("create schema", func(t *testing.T) {
				assert.Equal(t, 1, len(clusterInstance.Keyspaces[0].Shards))
				testWithInitialSchema(t)
			})
			t.Run("prepare table", func(t *testing.T) {
				if testcase.prepareStatement != "" {
					fullStatement := fmt.Sprintf("alter table %s %s", tableName, testcase.prepareStatement)
					onlineddl.VtgateExecDDL(t, &vtParams, directDDLStrategy, fullStatement, "")
				}
			})
			t.Run("init table data", func(t *testing.T) {
				initTable(t)
			})
			t.Run("migrate", func(t *testing.T) {
				require.NotEmpty(t, testcase.alterStatement)

				hintText := fmt.Sprintf("hint-after-alter-%d", rand.Int31n(int32(maxTableRows)))
				hintStatement := fmt.Sprintf(alterHintStatement, hintText)
				fullStatement := fmt.Sprintf("%s, %s", hintStatement, testcase.alterStatement)

				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					runMultipleConnections(ctx, t, testcase.autoIncInsert)
				}()
				uuid := testOnlineDDLStatement(t, fullStatement, onlineDDLStrategy, "vtgate", hintText)
				expectStatus := schema.OnlineDDLStatusComplete
				if testcase.expectFailure {
					expectStatus = schema.OnlineDDLStatusFailed
				}
				status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, waitForStatusTimeout, expectStatus)
				fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
				onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, expectStatus)
				cancel() // will cause runMultipleConnections() to terminate
				wg.Wait()
				if !testcase.expectFailure {
					testCompareBeforeAfterTables(t, testcase.autoIncInsert)
				}

				rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
				for _, row := range rs.Named().Rows {
					assert.Equal(t, testcase.expectAddedUniqueKeys, row.AsInt64("added_unique_keys", 0), "expectAddedUniqueKeys")
					assert.Equal(t, testcase.expectRemovedUniqueKeys, row.AsInt64("removed_unique_keys", 0), "expectRemovedUniqueKeys")
				}
			})
		})
	}
}

func testWithInitialSchema(t *testing.T) {
	// Create the stress table
	for _, statement := range cleanupStatements {
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, statement)
		require.Nil(t, err)
	}
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, createStatement)
	require.Nil(t, err)

	// Check if table is created
	checkTable(t, tableName)
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

	status := schema.OnlineDDLStatusComplete
	if !strategySetting.Strategy.IsDirect() {
		status = onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, waitForStatusTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectHint != "" && status == schema.OnlineDDLStatusComplete {
		checkMigratedTable(t, afterTableName, expectHint)
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

func generateInsert(t *testing.T, conn *mysql.Conn, autoIncInsert bool) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, id, -id, id, id, nextOpOrder())
	if autoIncInsert {
		id = rand.Int31()
		query = fmt.Sprintf(insertRowAutoIncStatement, -id, id, id, nextOpOrder())
	}
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err == nil && qr != nil {
		assert.Less(t, qr.RowsAffected, uint64(2))
	}
	return err
}

func generateUpdate(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, nextOpOrder(), id)
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err == nil && qr != nil {
		assert.Less(t, qr.RowsAffected, uint64(2))
	}
	return err
}

func generateDelete(t *testing.T, conn *mysql.Conn) error {
	id := rand.Int31n(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, id)
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err == nil && qr != nil {
		assert.Less(t, qr.RowsAffected, uint64(2))
	}
	return err
}

func runSingleConnection(ctx context.Context, t *testing.T, autoIncInsert bool, done *int64) {
	log.Infof("Running single connection")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1, false)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1, false)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set innodb_lock_wait_timeout=1", 1, false)
	require.Nil(t, err)

	periodicRest := timer.NewRateLimiter(time.Second)
	defer periodicRest.Stop()
	for {
		if atomic.LoadInt64(done) == 1 {
			log.Infof("Terminating single connection")
			return
		}
		switch rand.Int31n(3) {
		case 0:
			err = generateInsert(t, conn, autoIncInsert)
		case 1:
			err = generateUpdate(t, conn)
		case 2:
			err = generateDelete(t, conn)
		}
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") {
				// Table renamed to _before, due to -vreplication-test-suite flag
				err = nil
			}
			if sqlErr, ok := err.(*mysql.SQLError); ok {
				switch sqlErr.Number() {
				case mysql.ERLockDeadlock:
					// That's fine. We create a lot of contention; some transactions will deadlock and
					// rollback. It happens, and we can ignore those and keep on going.
					err = nil
				}
			}
		}
		assert.Nil(t, err)
		time.Sleep(singleConnectionSleepInterval)
		// Most o fthe time, we want the load to be high, so as to create real stress and potentially
		// expose bugs in vreplication (the objective of this test!).
		// However, some platforms (GitHub CI) can suffocate from this load. We choose to keep the load
		// high, when it runs, but then also take a periodic break and let the system recover.
		// We prefer this over reducing the load in general. In our method here, we have full load 90% of
		// the time, then relaxation 10% of the time.
		periodicRest.Do(func() error {
			time.Sleep(time.Second * periodicSleepPercent / 100)
			return nil
		})
	}
}

func runMultipleConnections(ctx context.Context, t *testing.T, autoIncInsert bool) {
	log.Infof("Running multiple connections")
	var done int64
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSingleConnection(ctx, t, autoIncInsert, &done)
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

	resetOpOrder()

	_, err = conn.ExecuteFetch(truncateStatement, 1000, true)
	require.Nil(t, err)

	for i := 0; i < maxTableRows/2; i++ {
		generateInsert(t, conn, false)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateUpdate(t, conn)
	}
	for i := 0; i < maxTableRows/4; i++ {
		generateDelete(t, conn)
	}
	{
		// Validate table is populated
		rs, err := conn.ExecuteFetch(selectCountFromTable, 1000, true)
		require.Nil(t, err)
		row := rs.Named().Row()
		require.NotNil(t, row)

		count := row.AsInt64("c", 0)
		require.NotZero(t, count)
		require.Less(t, count, int64(maxTableRows))

		fmt.Printf("# count rows in table: %d\n", count)
	}
}

// testCompareBeforeAfterTables validates that stress_test_before and stress_test_after contents are non empty and completely identical
func testCompareBeforeAfterTables(t *testing.T, autoIncInsert bool) {
	var countBefore int64
	{
		// Validate after table is populated
		rs := onlineddl.VtgateExecQuery(t, &vtParams, selectCountFromTableBefore, "")
		row := rs.Named().Row()
		require.NotNil(t, row)

		countBefore = row.AsInt64("c", 0)
		require.NotZero(t, countBefore)
		if !autoIncInsert {
			require.Less(t, countBefore, int64(maxTableRows))
		}
		fmt.Printf("# count rows in table (before): %d\n", countBefore)
	}
	var countAfter int64
	{
		// Validate after table is populated
		rs := onlineddl.VtgateExecQuery(t, &vtParams, selectCountFromTableAfter, "")
		row := rs.Named().Row()
		require.NotNil(t, row)

		countAfter = row.AsInt64("c", 0)
		require.NotZero(t, countAfter)
		if !autoIncInsert {
			require.Less(t, countAfter, int64(maxTableRows))
		}
		fmt.Printf("# count rows in table (after): %d\n", countAfter)
	}
	{
		rs := onlineddl.VtgateExecQuery(t, &vtParams, selectMaxOpOrderFromTableBefore, "")
		row := rs.Named().Row()
		require.NotNil(t, row)

		maxOpOrder := row.AsInt64("m", 0)
		fmt.Printf("# max op_order in table (before): %d\n", maxOpOrder)
	}
	{
		rs := onlineddl.VtgateExecQuery(t, &vtParams, selectMaxOpOrderFromTableAfter, "")
		row := rs.Named().Row()
		require.NotNil(t, row)

		maxOpOrder := row.AsInt64("m", 0)
		fmt.Printf("# max op_order in table (after): %d\n", maxOpOrder)
	}

	{
		selectBeforeFile := onlineddl.CreateTempScript(t, selectBeforeTable)
		defer os.Remove(selectBeforeFile)
		beforeOutput := onlineddl.MysqlClientExecFile(t, mysqlParams(), os.TempDir(), "", selectBeforeFile)
		beforeOutput = strings.TrimSpace(beforeOutput)
		require.NotEmpty(t, beforeOutput)
		assert.Equal(t, countBefore, int64(len(strings.Split(beforeOutput, "\n"))))

		selectAfterFile := onlineddl.CreateTempScript(t, selectAfterTable)
		defer os.Remove(selectAfterFile)
		afterOutput := onlineddl.MysqlClientExecFile(t, mysqlParams(), os.TempDir(), "", selectAfterFile)
		afterOutput = strings.TrimSpace(afterOutput)
		require.NotEmpty(t, afterOutput)
		assert.Equal(t, countAfter, int64(len(strings.Split(afterOutput, "\n"))))

		require.Equal(t, beforeOutput, afterOutput, "results mismatch: (%s) and (%s)", selectBeforeTable, selectAfterTable)
	}
}
