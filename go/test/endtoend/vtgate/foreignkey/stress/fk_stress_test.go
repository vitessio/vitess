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
	"math/rand/v2"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
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

	insertsFKErrors, updatesFKErrors, deletesFKErrors             int64
	sampleInsertFKError, sampleUpdateFKError, sampleDeleteFKError error
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

	w.insertsFKErrors = 0
	w.updatesFKErrors = 0
	w.deletesFKErrors = 0
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
	replicaNoFK     *cluster.Vttablet
	replicaFK       *cluster.Vttablet
	vtParams        mysql.ConnParams

	onlineDDLStrategy     = "vitess --unsafe-allow-foreign-keys --cut-over-threshold=15s"
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	parentTableName       = "stress_parent"
	childTableName        = "stress_child"
	child2TableName       = "stress_child2"
	grandchildTableName   = "stress_grandchild"
	nofkTableName         = "stress_nofk"
	tableNames            = []string{parentTableName, childTableName, child2TableName, grandchildTableName, nofkTableName}
	reverseTableNames     []string

	seedOnce sync.Once

	referenceActionMap = map[sqlparser.ReferenceAction]string{
		sqlparser.NoAction: "NO ACTION",
		sqlparser.Cascade:  "CASCADE",
		sqlparser.SetNull:  "SET NULL",
	}
	referenceActions = []sqlparser.ReferenceAction{sqlparser.NoAction, sqlparser.SetNull, sqlparser.Cascade}
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
		CREATE TABLE stress_nofk (
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
			CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES stress_parent (id) ON DELETE %s ON UPDATE %s
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
			CONSTRAINT child2_parent_fk FOREIGN KEY (parent_id) REFERENCES stress_parent (id) ON DELETE %s ON UPDATE %s
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
			CONSTRAINT grandchild_child_fk FOREIGN KEY (parent_id) REFERENCES stress_child (id) ON DELETE %s ON UPDATE %s
		) ENGINE=InnoDB
		`,
	}
	alterAddFKStatement = `
		ALTER TABLE stress_nofk add CONSTRAINT stress_nofk_parent_fk FOREIGN KEY (parent_id) REFERENCES stress_parent (id) ON DELETE NO ACTION ON UPDATE NO ACTION
	`
	alterDropFKStatement = `
		ALTER TABLE stress_nofk drop FOREIGN KEY stress_nofk_parent_fk
	`
	dropConstraintsStatements = []string{
		`ALTER TABLE stress_child DROP CONSTRAINT child_parent_fk`,
		`ALTER TABLE stress_child2 DROP CONSTRAINT child2_parent_fk`,
		`ALTER TABLE stress_grandchild DROP CONSTRAINT grandchild_child_fk`,
	}
	alterHintStatement = `
		ALTER TABLE %s modify hint_col varchar(64) not null default '%s'
	`
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
	selectOrphanedRowsNoFK = `
		select stress_nofk.id from stress_nofk left join stress_parent on (stress_parent.id = stress_nofk.parent_id) where stress_parent.id is null
	`
	deleteAllStatement = `
		DELETE FROM %s
	`
	writeMetrics = map[string]*WriteMetrics{}
)

const (
	maxTableRows         = 4096
	workloadDuration     = 5 * time.Second
	migrationWaitTimeout = 60 * time.Second
)

// The following variables are fit for a local, strong developer box.
// The test overrides these into more relaxed values if running on GITHUB_ACTIONS,
// seeing that GitHub CI is much weaker.
var (
	countIterations = 3
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
			"--migration_check_interval", "5s",
			"--watch_replication_stream",
		}
		clusterInstance.VtGateExtraArgs = []string{}

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
			VSchema: `{
				"sharded": false,
				"foreignKeyMode": "managed"
			}`,
		}

		// We will use a replica to confirm that vtgate's cascading works correctly.
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 2, false); err != nil {
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
	case replicaNoFK:
		return "replicaNoFK"
	case replicaFK:
		return "replicaFK"
	default:
		assert.FailNowf(t, "unknown tablet", "%v, type=%v", tablet.Alias, tablet.Type)
	}
	return ""
}

func validateReplicationIsHealthy(t *testing.T, tablet *cluster.Vttablet) (result bool) {
	t.Run(tabletTestName(t, tablet), func(t *testing.T) {
		result = cluster.ValidateReplicationIsHealthy(t, tablet)
	})
	return result
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

func waitForReplicaCatchup(t *testing.T, ctx context.Context, replica *cluster.Vttablet, pos replication.Position) {
	for {
		replicaPos := getTabletPosition(t, replica)
		if replicaPos.GTIDSet.Contains(pos.GTIDSet) {
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

func waitForReplicationCatchup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	primaryPos := getTabletPosition(t, primary)
	var wg sync.WaitGroup
	for _, replica := range []*cluster.Vttablet{replicaNoFK, replicaFK} {
		replica := replica
		wg.Add(1)
		go func() {
			waitForReplicaCatchup(t, ctx, replica, primaryPos)
			wg.Done()
		}()
	}
	wg.Wait()
}

func validateMetrics(t *testing.T, tcase *testCase) {
	for _, workloadTable := range tableNames {
		t.Run(workloadTable, func(t *testing.T) {
			t.Run("fk errors", func(t *testing.T) {
				testSelectTableFKErrors(t, workloadTable, tcase)
			})
			var primaryRows, replicaNoFKRows, replicaFKRows int64
			t.Run(tabletTestName(t, primary), func(t *testing.T) {
				primaryRows = testSelectTableMetrics(t, primary, workloadTable, tcase)
			})
			t.Run(tabletTestName(t, replicaNoFK), func(t *testing.T) {
				replicaNoFKRows = testSelectTableMetrics(t, replicaNoFK, workloadTable, tcase)
			})
			t.Run(tabletTestName(t, replicaFK), func(t *testing.T) {
				replicaFKRows = testSelectTableMetrics(t, replicaFK, workloadTable, tcase)
			})
			t.Run("compare primary and replicas", func(t *testing.T) {
				assert.Equal(t, primaryRows, replicaNoFKRows)
				assert.Equal(t, primaryRows, replicaFKRows)
			})
		})
	}
}

func TestInitialSetup(t *testing.T) {
	shards = clusterInstance.Keyspaces[0].Shards
	require.Equal(t, 1, len(shards))
	require.Equal(t, 3, len(shards[0].Vttablets)) // primary, no-fk replica, fk replica
	primary = shards[0].Vttablets[0]
	require.NotNil(t, primary)
	replicaNoFK = shards[0].Vttablets[1]
	require.NotNil(t, replicaNoFK)
	require.NotEqual(t, primary.Alias, replicaNoFK.Alias)
	replicaFK = shards[0].Vttablets[2]
	require.NotNil(t, replicaFK)
	require.NotEqual(t, primary.Alias, replicaFK.Alias)
	require.NotEqual(t, replicaNoFK.Alias, replicaFK.Alias)

	reverseTableNames = slices.Clone(tableNames)
	slices.Reverse(reverseTableNames)
	require.ElementsMatch(t, tableNames, reverseTableNames)

	for _, tableName := range tableNames {
		writeMetrics[tableName] = &WriteMetrics{}
	}
}

type testCase struct {
	onDeleteAction       sqlparser.ReferenceAction
	onUpdateAction       sqlparser.ReferenceAction
	workload             bool
	onlineDDLTable       string
	reseedInsertIgnore   bool
	preStatement         string
	alterStatement       string
	createTableHint      string
	notes                string // human readable, added to test name
	skipNofkOrphanedRows bool
}

// ExecuteFKTest runs a single test case, which can be:
// - With/out workload
// - Either one of ON DELETE actions
// - Either one of ON UPDATE actions
// - Potentially running an Online DDL on an indicated table (this will not work in Vanilla MySQL, see https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/)
func ExecuteFKTest(t *testing.T, tcase *testCase) {
	workloadName := "static data"
	if tcase.workload {
		workloadName = "workload"
	}
	testName := fmt.Sprintf("%s/del=%s/upd=%s", workloadName, referenceActionMap[tcase.onDeleteAction], referenceActionMap[tcase.onUpdateAction])
	testOnlineDDL := (tcase.onlineDDLTable != "")
	if testOnlineDDL {
		testName = fmt.Sprintf("%s/ddl=%s", testName, tcase.onlineDDLTable)
	}
	if tcase.notes != "" {
		testName = fmt.Sprintf("%s/%s", testName, tcase.notes)
	}
	t.Run(testName, func(t *testing.T) {
		ctx := context.Background()

		t.Run("create schema", func(t *testing.T) {
			createInitialSchema(t, tcase)
		})
		t.Run("init tables", func(t *testing.T) {
			populateTables(t, tcase)
		})
		if tcase.workload {
			t.Run("workload", func(t *testing.T) {
				// The workload for a 16 vCPU machine is:
				// - Concurrency of 16
				// - 15ms interval between queries for each connection
				// As the number of vCPUs decreases, so do we decrease concurrency, and increase intervals. For example, on a 8 vCPU machine
				// we run concurrency of 8 and interval of 4ms. On a 4 vCPU machine we run concurrency of 4 and interval of 8ms.
				maxConcurrency := max((len(tableNames) * 2), runtime.NumCPU()*2)
				sleepModifier := 16.0 / float64(maxConcurrency)
				baseSleepInterval := 15 * time.Millisecond
				singleConnectionSleepIntervalNanoseconds := float64(baseSleepInterval.Nanoseconds()) * sleepModifier
				sleepInterval := time.Duration(int64(singleConnectionSleepIntervalNanoseconds))
				if testOnlineDDL {
					sleepInterval = sleepInterval * 2
					maxConcurrency = max(1, maxConcurrency/2)
				}
				t.Logf("==== workload setup: maxConcurrency=%v, sleepInterval=%v", maxConcurrency, sleepInterval)

				ctx, cancel := context.WithTimeout(ctx, workloadDuration)
				defer cancel()

				var wg sync.WaitGroup
				for i := 0; i < maxConcurrency; i++ {
					tableName := tableNames[i%len(tableNames)]
					wg.Add(1)
					go func() {
						defer wg.Done()
						runSingleConnection(ctx, t, tableName, sleepInterval)
					}()
				}

				if testOnlineDDL {
					t.Run("migrating", func(t *testing.T) {
						// This only works on patched MySQL
						hint := tcase.createTableHint
						alterStatement := tcase.alterStatement
						if alterStatement == "" {
							hint = "hint-alter"
							alterStatement = fmt.Sprintf(alterHintStatement, tcase.onlineDDLTable, hint)
						}
						t.Logf("alter statement: %v, hint: %v", alterStatement, hint)
						uuid := testOnlineDDLStatement(t, alterStatement, onlineDDLStrategy, "vtgate", hint)
						ok := onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
						require.True(t, ok) // or else don't attempt to cleanup artifacts
						t.Run("cleanup artifacts", func(t *testing.T) {
							rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
							require.NotNil(t, rs)
							row := rs.Named().Row()
							require.NotNil(t, row)

							artifacts := textutil.SplitDelimitedList(row.AsString("artifacts", ""))
							for _, artifact := range artifacts {
								t.Run(artifact, func(t *testing.T) {
									err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, "drop table if exists "+artifact)
									require.NoError(t, err)
								})
							}
						})
					})
				}
				wg.Wait()
			})
		}
		t.Run("wait for replicas", func(t *testing.T) {
			waitForReplicationCatchup(t)
		})
		validateTableDefinitions(t, testOnlineDDL)
		t.Run("validate metrics", func(t *testing.T) {
			validateMetrics(t, tcase)
		})
		t.Run("validate replication health", func(t *testing.T) {
			validateReplicationIsHealthy(t, replicaNoFK)
			validateReplicationIsHealthy(t, replicaFK)
		})
		t.Run("validate fk", func(t *testing.T) {
			testFKIntegrity(t, primary, tcase)
			testFKIntegrity(t, replicaNoFK, tcase)
			testFKIntegrity(t, replicaFK, tcase)
		})
	})
}

func TestStressFK(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("validate replication health", func(t *testing.T) {
		validateReplicationIsHealthy(t, replicaNoFK)
		validateReplicationIsHealthy(t, replicaFK)
	})

	runOnlineDDL := false
	t.Run("check 'rename_table_preserve_foreign_key' variable", func(t *testing.T) {
		// Online DDL is not possible on vanilla MySQL 8.0 for reasons described in https://vitess.io/blog/2021-06-15-online-ddl-why-no-fk/.
		// However, Online DDL is made possible in via these changes:
		// - https://github.com/planetscale/mysql-server/commit/bb777e3e86387571c044fb4a2beb4f8c60462ced
		// - https://github.com/planetscale/mysql-server/commit/c2f1344a6863518d749f2eb01a4c74ca08a5b889
		// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps3.
		// Said changes introduce a new behavior for `RENAME TABLE`. When at least two tables are being renamed in the statement,
		// and when at least one table uses internal vitess naming, then a `RENAME TABLE` to a FK parent "pins" the children's
		// foreign keys to the table name rather than the table pointer. Which means after the RENAME,
		// the children will point to the newly instated table rather than the original, renamed table.
		// For FK children, the MySQL changes simply ignore any Vitess-internal table.
		//
		// The variable 'rename_table_preserve_foreign_key' serves as an indicator to the functionality's availability,
		// and at this time changing its value does not change any behavior.
		//
		// In this stress test, we enable Online DDL if the variable 'rename_table_preserve_foreign_key' is present. The Online DDL mechanism will in turn
		// query for this variable, and manipulate it, when starting the migration and when cutting over.
		rs, err := primary.VttabletProcess.QueryTablet("show global variables like 'rename_table_preserve_foreign_key'", keyspaceName, false)
		require.NoError(t, err)
		runOnlineDDL = len(rs.Rows) > 0
		t.Logf("MySQL support for 'rename_table_preserve_foreign_key': %v", runOnlineDDL)
	})
	if val, present := os.LookupEnv("FK_STRESS_ONLINE_DDL"); present && val != "" {
		// A way to force execution of Online DDL. Online DDL won't work correctly with vanilla MySQL. See above.
		runOnlineDDL = true
	}

	// Without workload ; with workload
	for _, workload := range []bool{false, true} {
		// For any type of ON DELETE action
		for _, actionDelete := range referenceActions {
			// For any type of ON UPDATE action
			for _, actionUpdate := range referenceActions {
				tcase := &testCase{
					workload:       workload,
					onDeleteAction: actionDelete,
					onUpdateAction: actionUpdate,
				}
				ExecuteFKTest(t, tcase)
			}
		}
	}

	if runOnlineDDL {
		// Running Online DDL on all test tables. We don't use all of the combinations
		// presented above; we will run with workload, and suffice with same ON DELETE - ON UPDATE actions.
		for _, action := range referenceActions {
			for _, table := range tableNames {
				tcase := &testCase{
					notes:          "standard alter",
					workload:       true,
					onDeleteAction: action,
					onUpdateAction: action,
					onlineDDLTable: table,
				}
				ExecuteFKTest(t, tcase)
			}
		}
		// Specific extra tests:
		{
			// Add foreign key constraint to a table without one.
			tcase := &testCase{
				notes:           "add fk",
				workload:        true,
				onDeleteAction:  sqlparser.NoAction,
				onUpdateAction:  sqlparser.NoAction,
				onlineDDLTable:  "stress_nofk",
				alterStatement:  alterAddFKStatement,
				createTableHint: "stress_nofk_parent_fk",
			}
			ExecuteFKTest(t, tcase)
		}
		{
			// Drop a constraint, leaving the table without any foreign keys.
			// We use `skipNofkOrphanedRows` because for the duration of the migration,
			// `stress_nofk` table will be compliant with `stress_parent`. It's only at
			// the very end of the test, just as the migration completes, that the workload
			// has the chance to inject orphaned rows. But then the test terminates immediately
			// and so we can't be sure that orphaned rows will exist.
			tcase := &testCase{
				notes:                "drop fk",
				workload:             true,
				onDeleteAction:       sqlparser.NoAction,
				onUpdateAction:       sqlparser.NoAction,
				onlineDDLTable:       "stress_nofk",
				preStatement:         alterAddFKStatement,
				reseedInsertIgnore:   true,
				alterStatement:       alterDropFKStatement,
				createTableHint:      "parent_id",
				skipNofkOrphanedRows: true,
			}
			ExecuteFKTest(t, tcase)
		}
	}
}

func validateTableDefinitions(t *testing.T, afterOnlineDDL bool) {
	t.Run("validate definitions", func(t *testing.T) {
		for _, tableName := range []string{childTableName, child2TableName, grandchildTableName} {
			t.Run(tableName, func(t *testing.T) {
				childFKFollowedParentRenameMsg := "found traces of internal vitess table name, suggesting Online DDL on parent table caused this child table to follow the renames parent. 'rename_table_preserve_foreign_key' should have prevented this"
				var primaryStmt string
				t.Run(tabletTestName(t, primary), func(t *testing.T) {
					primaryStmt = getCreateTableStatement(t, primary, tableName)
					assert.NotEmpty(t, primaryStmt)
					assert.Contains(t, primaryStmt, "CONSTRAINT")
					assert.NotContainsf(t, primaryStmt, "_vrepl", childFKFollowedParentRenameMsg)
					assert.NotContainsf(t, primaryStmt, "_vrp_", childFKFollowedParentRenameMsg)
				})
				t.Run(tabletTestName(t, replicaFK), func(t *testing.T) {
					stmt := getCreateTableStatement(t, replicaFK, tableName)
					assert.Contains(t, stmt, "CONSTRAINT")
					assert.Equal(t, primaryStmt, stmt)
					assert.NotContainsf(t, stmt, "_vrepl", childFKFollowedParentRenameMsg)
					assert.NotContainsf(t, stmt, "_vrp_", childFKFollowedParentRenameMsg)
				})
				t.Run(tabletTestName(t, replicaNoFK), func(t *testing.T) {
					stmt := getCreateTableStatement(t, replicaNoFK, tableName)
					// replicaNoFK does not have foreign keys, for the purpose of testing VTGate's cascading
					// of foreign key rules.
					// However, if we run Online DDL, the table will be swapped at the end of the migration.
					// We're not sure here exactly which table has been migrated. Was it this table's parent?
					// Or this table itself? Or an unrelated table? In case of Online DDL we don't want to
					// validate this replicas' schema, because it could be any one of several outcomes. And
					// we don't even care how this replica's schema looks like after the migration. Ths
					// schema was inconsistent with the Primary to begin with. We've already tested replicaFK
					// for correctness of the schema.
					if !afterOnlineDDL {
						assert.NotContains(t, stmt, "CONSTRAINT")
						assert.NotEqual(t, primaryStmt, stmt)
					}
				})
			})
		}
	})
}

// createInitialSchema creates the tables from scratch, and drops the foreign key constraints on the replica.
func createInitialSchema(t *testing.T, tcase *testCase) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	t.Run("dropping tables", func(t *testing.T) {
		for _, tableName := range reverseTableNames {
			err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, "drop table if exists "+tableName)
			require.NoError(t, err)
		}
	})
	t.Run("waiting for vschema deletions to apply", func(t *testing.T) {
		for _, tableName := range tableNames {
			utils.WaitForTableDeletions(t, clusterInstance.VtgateProcess, keyspaceName, tableName)
		}
	})
	t.Run("creating tables", func(t *testing.T) {
		// Create the stress tables
		var b strings.Builder
		for i, sql := range createStatements {
			switch i {
			case 0:
				// parent table, no foreign keys
				b.WriteString(sql)
			case 1:
				// stress_nofk, no foreign keys
				b.WriteString(sql)
			default:
				b.WriteString(fmt.Sprintf(sql, referenceActionMap[tcase.onDeleteAction], referenceActionMap[tcase.onUpdateAction]))
			}
			b.WriteString(";")
		}
		err := clusterInstance.VtctldClientProcess.ApplySchema(keyspaceName, b.String())
		require.NoError(t, err)
	})
	if tcase.preStatement != "" {
		t.Run("pre-statement", func(t *testing.T) {
			_, err = conn.ExecuteFetch(tcase.preStatement, 1, false)
			require.Nil(t, err)
		})
	}
	t.Run("wait for replication", func(t *testing.T) {
		waitForReplicationCatchup(t)
	})
	t.Run("validating tables: vttablet", func(t *testing.T) {
		// Check if table is created. Checked on tablets.
		checkTable(t, parentTableName, "hint_col")
		checkTable(t, childTableName, "hint_col")
		checkTable(t, child2TableName, "hint_col")
		checkTable(t, grandchildTableName, "hint_col")
		checkTable(t, nofkTableName, "hint_col")
	})
	t.Run("validating tables: vtgate", func(t *testing.T) {
		// Wait for tables to appear on VTGate
		waitForTable(t, parentTableName, conn)
		waitForTable(t, childTableName, conn)
		waitForTable(t, child2TableName, conn)
		waitForTable(t, grandchildTableName, conn)
		waitForTable(t, nofkTableName, conn)
	})
	t.Run("waiting for vschema definition to apply", func(t *testing.T) {
		for _, tableName := range tableNames {
			err := utils.WaitForColumn(t, clusterInstance.VtgateProcess, keyspaceName, tableName, "id")
			require.NoError(t, err)
		}
	})

	t.Run("dropping foreign keys on replica", func(t *testing.T) {
		for _, statement := range dropConstraintsStatements {
			_ = queryTablet(t, replicaNoFK, "set global super_read_only=0", "")
			_ = queryTablet(t, replicaNoFK, statement, "")
			_ = queryTablet(t, replicaNoFK, "set global super_read_only=1", "")
		}
	})
	validateTableDefinitions(t, false)
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
		uuid, err = clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(keyspaceName, alterStatement, cluster.ApplySchemaParams{DDLStrategy: ddlStrategy})
		assert.NoError(t, err)
	}
	uuid = strings.TrimSpace(uuid)
	fmt.Println("# Generated UUID (for debug purposes):")
	fmt.Printf("<%s>\n", uuid)

	strategySetting, err := schema.ParseDDLStrategy(ddlStrategy)
	assert.NoError(t, err)

	if !strategySetting.Strategy.IsDirect() {
		t.Logf("===== waiting for migration %v to conclude", uuid)
		status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, migrationWaitTimeout, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
		fmt.Printf("# Migration status (for debug purposes): <%s>\n", status)
	}

	if expectHint != "" {
		stmt, err := sqlparser.NewTestParser().Parse(alterStatement)
		require.NoError(t, err)
		ddlStmt, ok := stmt.(sqlparser.DDLStatement)
		require.True(t, ok)
		tableName := ddlStmt.GetTable().Name.String()
		checkTable(t, tableName, expectHint)
	}

	if !strategySetting.Strategy.IsDirect() {
		// let's see what FK tables have been renamed to
		rs := onlineddl.ReadMigrations(t, &vtParams, uuid)
		require.NotNil(t, rs)
		row := rs.Named().Row()
		require.NotNil(t, row)

		artifacts := textutil.SplitDelimitedList(row.AsString("artifacts", ""))
		for _, artifact := range artifacts {
			checkTable(t, artifact, "")
		}
	}

	return uuid
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
func checkTable(t *testing.T, showTableName string, expectHint string) {
	for _, tablet := range shards[0].Vttablets {
		checkTablesCount(t, tablet, showTableName, 1)
		if expectHint != "" {
			createStatement := getCreateTableStatement(t, tablet, showTableName)
			assert.Contains(t, createStatement, expectHint)
		}
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

func isFKError(err error) bool {
	if err == nil {
		return false
	}
	sqlErr, ok := err.(*sqlerror.SQLError)
	if !ok {
		return false
	}

	// Let's try and account for all known errors:
	switch sqlErr.Number() {
	case sqlerror.ERDupEntry: // happens since we hammer the tables randomly
		return false
	case sqlerror.ERTooManyUserConnections: // can happen in Online DDL cut-over
		return false
	case sqlerror.ERUnknownError: // happens when query buffering times out
		return false
	case sqlerror.ERQueryInterrupted: // cancelled due to context expiration
		return false
	case sqlerror.ERLockDeadlock:
		return false // bummer, but deadlocks can happen, it's a legit error.
	case sqlerror.ERLockNowait:
		return false // For some queries we use NOWAIT. Bummer, but this can happen, it's a legit error.
	case sqlerror.ERNoReferencedRow,
		sqlerror.ERRowIsReferenced,
		sqlerror.ERRowIsReferenced2,
		sqlerror.ErNoReferencedRow2:
		return true
	case sqlerror.ERNotSupportedYet:
		return true
	}
	// Unknown error
	fmt.Printf("Unexpected error detected in isFKError: %v\n", err)
	// Treat it as if it's a FK error
	return true
}

func generateInsert(t *testing.T, tableName string, conn *mysql.Conn) error {
	id := rand.Int32N(int32(maxTableRows))
	parentId := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(insertRowStatement, tableName, id, parentId)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].insertsAttempts++
		if err != nil {
			writeMetrics[tableName].insertsFailures++
			if isFKError(err) {
				writeMetrics[tableName].insertsFKErrors++
				writeMetrics[tableName].sampleInsertFKError = err
			}
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
	// Most of the UPDATEs we run are "normal" updates, but the minority will actually change the
	// `id` column itself, which is the FOREIGN KEY parent column for some of the tables.
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(updateRowStatement, tableName, id)
	if tableName == parentTableName || tableName == childTableName {
		if rand.IntN(4) == 0 {
			updatedId := rand.Int32N(int32(maxTableRows))
			query = fmt.Sprintf(updateRowIdStatement, tableName, updatedId, id)
		}
	}
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].updatesAttempts++
		if err != nil {
			writeMetrics[tableName].updatesFailures++
			if isFKError(err) {
				writeMetrics[tableName].updatesFKErrors++
				writeMetrics[tableName].sampleUpdateFKError = err
			}
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
	id := rand.Int32N(int32(maxTableRows))
	query := fmt.Sprintf(deleteRowStatement, tableName, id)
	qr, err := conn.ExecuteFetch(query, 1000, true)

	func() {
		writeMetrics[tableName].mu.Lock()
		defer writeMetrics[tableName].mu.Unlock()

		writeMetrics[tableName].deletesAttempts++
		if err != nil {
			writeMetrics[tableName].deletesFailures++
			if isFKError(err) {
				writeMetrics[tableName].deletesFKErrors++
				writeMetrics[tableName].sampleDeleteFKError = err
			}
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

func runSingleConnection(ctx context.Context, t *testing.T, tableName string, sleepInterval time.Duration) {
	log.Infof("Running single connection on %s", tableName)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("set autocommit=1", 1000, true)
	require.Nil(t, err)
	_, err = conn.ExecuteFetch("set transaction isolation level read committed", 1000, true)
	require.Nil(t, err)

	for {
		switch rand.Int32N(3) {
		case 0:
			_ = generateInsert(t, tableName, conn)
		case 1:
			_ = generateUpdate(t, tableName, conn)
		case 2:
			_ = generateDelete(t, tableName, conn)
		}
		select {
		case <-ctx.Done():
			log.Infof("Terminating single connection")
			return
		case <-time.After(sleepInterval):
		}
	}
}

func wrapWithNoFKChecks(sql string) string {
	return fmt.Sprintf("set foreign_key_checks=0; %s; set foreign_key_checks=1;", sql)
}

// populateTables randomly populates all test tables. This is done sequentially.
func populateTables(t *testing.T, tcase *testCase) {
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
	// In an ideal world we would randomly re-seed the tables in each and every instance of the test.
	// In reality, that takes a lot of time, and while the seeding is important, it's not the heart of
	// the test. To that effect, the seeding works as follows:
	// - First ever time, we randomly seed the tables (running thousands of queries). We then create *_seed
	//   tables and clone the data in those seed tables.
	// - 2nd test and forward: we just copy over the rows from the *_seed tables.
	tablesSeeded := false
	seedOnce.Do(func() {
		for _, tableName := range tableNames {
			t.Run(tableName, func(t *testing.T) {
				t.Run("populating", func(t *testing.T) {
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
				})
				t.Run("creating seed", func(t *testing.T) {
					// We create the seed table in the likeness of stress_parent, because that's the only table
					// that doesn't have FK constraints.
					{
						createSeedQuery := fmt.Sprintf("create table %s_seed like %s", tableName, parentTableName)
						_, err := conn.ExecuteFetch(createSeedQuery, 1000, true)
						require.NoError(t, err)
					}
					{
						seedQuery := fmt.Sprintf("insert into %s_seed select * from %s", tableName, tableName)
						_, err := conn.ExecuteFetch(seedQuery, 1000, true)
						require.NoError(t, err)
					}
					{
						validationQuery := fmt.Sprintf("select count(*) as c from %s_seed", tableName)
						rs, err := conn.ExecuteFetch(validationQuery, 1000, true)
						require.NoError(t, err)
						row := rs.Named().Row()
						require.NotNil(t, row)
						require.NotZero(t, row.AsInt64("c", 0))
					}
				})
			})
		}
		tablesSeeded = true
	})
	if !tablesSeeded {
		t.Run("reseeding", func(t *testing.T) {
			for _, tableName := range tableNames {
				ignoreModifier := ""
				if tcase.reseedInsertIgnore {
					ignoreModifier = "ignore"
				}
				seedQuery := fmt.Sprintf("insert %s into %s select * from %s_seed", ignoreModifier, tableName, tableName)
				_, err := conn.ExecuteFetch(seedQuery, 1000, true)
				require.NoError(t, err)
			}
		})
	}

	t.Run("validating table rows", func(t *testing.T) {
		for _, tableName := range tableNames {
			validationQuery := fmt.Sprintf(selectCountRowsStatement, tableName)
			rs, err := conn.ExecuteFetch(validationQuery, 1000, true)
			require.NoError(t, err)
			row := rs.Named().Row()
			require.NotNil(t, row)
			numRows := row.AsInt64("num_rows", 0)
			sumUpdates := row.AsInt64("sum_updates", 0)
			require.NotZero(t, numRows)
			if !tablesSeeded {
				// We cloned the data from *_seed tables. This means we didn't populate writeMetrics. Now,
				// this function only takes care of the base seed. We will later on run a stress workload on
				// these tables, at the end of which we will examine the writeMetrics. We thus have to have those
				// metrics consistent with the cloned data. It's a bit ugly, but we inject fake writeMetrics.
				writeMetrics[tableName].deletes = 1
				writeMetrics[tableName].inserts = numRows + writeMetrics[tableName].deletes
				writeMetrics[tableName].updates = sumUpdates + writeMetrics[tableName].deletes
			}
		}
	})
}

// testSelectTableMetrics cross references the known metrics (number of successful insert/delete/updates) on each table, with the
// actual number of rows and with the row values on those tables.
// With CASCADE/SET NULL rules we can't do the comparison, because child tables are implicitly affected by the cascading rules,
// and the values do not match what reported to us when we UPDATE/DELETE on the parent tables.
func testSelectTableMetrics(
	t *testing.T,
	tablet *cluster.Vttablet,
	tableName string,
	tcase *testCase,
) int64 {
	switch tcase.onDeleteAction {
	case sqlparser.Cascade, sqlparser.SetNull:
		if tableName != parentTableName {
			// We can't validate those tables because they will have been affected by cascading rules.
			return 0
		}
	}
	// metrics are unaffected by value of onUpdateAction.

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

// testSelectTableFKErrors
func testSelectTableFKErrors(
	t *testing.T,
	tableName string,
	tcase *testCase,
) {
	writeMetrics[tableName].mu.Lock()
	defer writeMetrics[tableName].mu.Unlock()

	if tcase.onDeleteAction == sqlparser.Cascade {
		assert.Zerof(t, writeMetrics[tableName].deletesFKErrors, "unexpected foreign key errors for DELETEs in ON DELETE CASCADE. Sample error: %v", writeMetrics[tableName].sampleDeleteFKError)
	}
	if tcase.onUpdateAction == sqlparser.Cascade {
		assert.Zerof(t, writeMetrics[tableName].updatesFKErrors, "unexpected foreign key errors for UPDATEs in ON UPDATE CASCADE. Sample error: %v", writeMetrics[tableName].sampleUpdateFKError)
	}
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
func testFKIntegrity(
	t *testing.T,
	tablet *cluster.Vttablet,
	tcase *testCase,
) {
	testName := tabletTestName(t, tablet)
	t.Run(testName, func(t *testing.T) {
		t.Run("matching parent-child rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsChild, "")
			assert.NotZero(t, len(rs.Rows))
		})
		t.Run("matching parent-child2 rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsChild2, "")
			assert.NotZero(t, len(rs.Rows))
		})
		t.Run("matching child-grandchild rows", func(t *testing.T) {
			rs := queryTablet(t, tablet, selectMatchingRowsGrandchild, "")
			assert.NotZero(t, len(rs.Rows))
		})
		if tcase.onDeleteAction != sqlparser.SetNull && tcase.onUpdateAction != sqlparser.SetNull {
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
			if !tcase.skipNofkOrphanedRows {
				t.Run("parent-nofk orphaned rows", func(t *testing.T) {
					rs := queryTablet(t, tablet, selectOrphanedRowsNoFK, "")
					// Expect orphaned rows!
					assert.NotZero(t, len(rs.Rows))
				})
			}
		}
	})
}
