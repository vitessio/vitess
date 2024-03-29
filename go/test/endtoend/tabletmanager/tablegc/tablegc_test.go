/*
Copyright 2020 The Vitess Authors.

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
package tablegc

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/gc"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	fastDropTable   bool
	sqlCreateTable  = `
		create table if not exists t1(
			id bigint not null auto_increment,
			value varchar(32),
			primary key(id)
		) Engine=InnoDB;
	`
	sqlCreateView = `
		create or replace view v1 as select * from t1
	`
	sqlSchema = []string{sqlCreateTable, sqlCreateView}

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
	}`

	tableTransitionExpiration = 10 * time.Second
	gcCheckInterval           = 2 * time.Second
	gcPurgeCheckInterval      = 2 * time.Second
	waitForTransitionTimeout  = 30 * time.Second
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"--lock_tables_timeout", "5s",
			"--watch_replication_stream",
			"--enable_replication_reporter",
			"--heartbeat_interval", "250ms",
			"--gc_check_interval", gcCheckInterval.String(),
			"--gc_purge_check_interval", gcPurgeCheckInterval.String(),
			"--table_gc_lifecycle", "hold,purge,evac,drop",
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: strings.Join(sqlSchema, ";"),
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func getTableRows(t *testing.T, tableName string) int64 {
	require.NotEmpty(t, tableName)
	query := `select count(*) as c from %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	rs, err := primaryTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	require.NoError(t, err)
	count := rs.Named().Row().AsInt64("c", 0)
	return count
}

func checkTableRows(t *testing.T, tableName string, expect int64) {
	count := getTableRows(t, tableName)
	assert.Equal(t, expect, count)
}

func populateTable(t *testing.T) {
	err := primaryTablet.VttabletProcess.QueryTabletMultiple(sqlSchema, keyspaceName, true)
	require.NoError(t, err)

	_, err = primaryTablet.VttabletProcess.QueryTablet("delete from t1", keyspaceName, true)
	require.NoError(t, err)
	_, err = primaryTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) values (null, md5(rand()))", keyspaceName, true)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = primaryTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) select null, md5(rand()) from t1", keyspaceName, true)
		require.NoError(t, err)
	}
	checkTableRows(t, "t1", 1024)
	{
		exists, _, err := tableExists("t1")
		require.NoError(t, err)
		require.True(t, exists)
	}
}

// tableExists sees that a given table exists in MySQL
func tableExists(exprs ...string) (exists bool, tableName string, err error) {
	if len(exprs) == 0 {
		return false, "", fmt.Errorf("empty table list")
	}
	var clauses []string
	for _, expr := range exprs {
		clauses = append(clauses, fmt.Sprintf("table_name like '%s'", expr))
	}
	clause := strings.Join(clauses, " or ")
	query := fmt.Sprintf(`select table_name as table_name from information_schema.tables where table_schema=database() and (%s)`, clause)
	rs, err := primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	if err != nil {
		return false, "", err
	}
	for _, row := range rs.Named().Rows {
		return true, row.AsString("table_name", ""), nil
	}
	return false, "", nil
}

func validateTableDoesNotExist(t *testing.T, tableExpr string) {
	ctx, cancel := context.WithTimeout(context.Background(), waitForTransitionTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		exists, foundTableName, err := tableExists(tableExpr)
		require.NoError(t, err)
		if !exists {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			assert.Failf(t, "validateTableDoesNotExist timed out, table %v still exists (%v)", tableExpr, foundTableName)
			return
		}
	}
}

func validateTableExists(t *testing.T, tableExpr string) {
	ctx, cancel := context.WithTimeout(context.Background(), waitForTransitionTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		exists, _, err := tableExists(tableExpr)
		require.NoError(t, err)
		if exists {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			assert.Failf(t, "validateTableExists timed out, table %v still does not exist", tableExpr)
			return
		}
	}
}

func validateAnyState(t *testing.T, expectNumRows int64, states ...schema.TableGCState) {
	t.Run(fmt.Sprintf("validateAnyState: expectNumRows=%v, states=%v", expectNumRows, states), func(t *testing.T) {
		timeout := gc.NextChecksIntervals[len(gc.NextChecksIntervals)-1] + 5*time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			// Attempt validation:
			for _, state := range states {
				expectTableToExist := true
				searchExpr := ""
				searchExpr2 := ""
				switch state {
				case schema.HoldTableGCState:
					searchExpr = `\_vt\_HOLD\_%`
					searchExpr2 = `\_vt\_hld\_%`
				case schema.PurgeTableGCState:
					searchExpr = `\_vt\_PURGE\_%`
					searchExpr2 = `\_vt\_prg\_%`
				case schema.EvacTableGCState:
					searchExpr = `\_vt\_EVAC\_%`
					searchExpr2 = `\_vt\_evc\_%`
				case schema.DropTableGCState:
					searchExpr = `\_vt\_DROP\_%`
					searchExpr2 = `\_vt\_drp\_%`
				case schema.TableDroppedGCState:
					searchExpr = `\_vt\_%`
					searchExpr2 = `\_vt\_%`
					expectTableToExist = false
				default:
					require.Failf(t, "unknown state", "%v", state)
				}
				exists, tableName, err := tableExists(searchExpr, searchExpr2)
				require.NoError(t, err)

				var foundRows int64
				if exists {
					foundRows = getTableRows(t, tableName)
					// Now that the table is validated, we can drop it (test cleanup)
					dropTable(t, tableName)
				}
				t.Logf("=== exists: %v, tableName: %v, rows: %v", exists, tableName, foundRows)
				if exists == expectTableToExist {
					// expectNumRows < 0 means "don't care"
					if expectNumRows < 0 || (expectNumRows == foundRows) {
						// All conditions are met
						return
					}
				}
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				assert.Failf(t, "timeout in validateAnyState", " waiting for any of these states: %v, expecting rows: %v", states, expectNumRows)
				return
			}
		}
	})
}

// dropTable drops a table
func dropTable(t *testing.T, tableName string) {
	query := `drop table if exists %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	_, err := primaryTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	require.NoError(t, err)
}

func TestCapability(t *testing.T) {
	mysqlVersion := onlineddl.GetMySQLVersion(t, clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet())
	require.NotEmpty(t, mysqlVersion)

	capableOf := mysql.ServerVersionCapableOf(mysqlVersion)
	require.NotNil(t, capableOf)
	var err error
	fastDropTable, err = capableOf(capabilities.FastDropTableFlavorCapability)
	require.NoError(t, err)
}

func TestPopulateTable(t *testing.T) {
	populateTable(t)
	validateTableExists(t, "t1")
	validateTableDoesNotExist(t, "no_such_table")
}

func generateRenameStatement(newFormat bool, fromTableName string, state schema.TableGCState, tm time.Time) (statement string, toTableName string, err error) {
	if newFormat {
		return schema.GenerateRenameStatement(fromTableName, state, tm)
	}
	return schema.GenerateRenameStatementOldFormat(fromTableName, state, tm)
}

func TestHold(t *testing.T) {
	for _, newNameFormat := range []bool{false, true} {
		t.Run(fmt.Sprintf("new format=%t", newNameFormat), func(t *testing.T) {
			populateTable(t)
			query, tableName, err := generateRenameStatement(newNameFormat, "t1", schema.HoldTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
			assert.NoError(t, err)

			_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
			assert.NoError(t, err)

			validateTableDoesNotExist(t, "t1")
			validateTableExists(t, tableName)

			time.Sleep(tableTransitionExpiration / 2)
			{
				// Table was created with +10s timestamp, so it should still exist
				validateTableExists(t, tableName)

				checkTableRows(t, tableName, 1024)
			}

			time.Sleep(tableTransitionExpiration)
			// We're now both beyond table's timestamp as well as a tableGC interval
			validateTableDoesNotExist(t, tableName)
			if fastDropTable {
				validateAnyState(t, -1, schema.DropTableGCState, schema.TableDroppedGCState)
			} else {
				validateAnyState(t, -1, schema.PurgeTableGCState, schema.EvacTableGCState, schema.DropTableGCState, schema.TableDroppedGCState)
			}
		})
	}
}

func TestEvac(t *testing.T) {
	for _, newNameFormat := range []bool{false, true} {
		t.Run(fmt.Sprintf("new format=%t", newNameFormat), func(t *testing.T) {
			var tableName string
			t.Run("setting up EVAC table", func(t *testing.T) {
				populateTable(t)
				var query string
				var err error
				query, tableName, err = generateRenameStatement(newNameFormat, "t1", schema.EvacTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
				assert.NoError(t, err)

				_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
				assert.NoError(t, err)

				validateTableDoesNotExist(t, "t1")
			})

			t.Run("validating before expiration", func(t *testing.T) {
				time.Sleep(tableTransitionExpiration / 2)
				// Table was created with +10s timestamp, so it should still exist
				if fastDropTable {
					// EVAC state is skipped in mysql 8.0.23 and beyond
					validateTableDoesNotExist(t, tableName)
				} else {
					validateTableExists(t, tableName)
					checkTableRows(t, tableName, 1024)
				}
			})

			t.Run("validating rows evacuated", func(t *testing.T) {
				// We're now both beyond table's timestamp as well as a tableGC interval
				validateTableDoesNotExist(t, tableName)
				// Table should be renamed as _vt_DROP_... and then dropped!
				validateAnyState(t, 0, schema.DropTableGCState, schema.TableDroppedGCState)
			})
		})
	}
}

func TestDrop(t *testing.T) {
	for _, newNameFormat := range []bool{false, true} {
		t.Run(fmt.Sprintf("new format=%t", newNameFormat), func(t *testing.T) {
			populateTable(t)
			query, tableName, err := generateRenameStatement(newNameFormat, "t1", schema.DropTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
			assert.NoError(t, err)

			_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
			assert.NoError(t, err)

			validateTableDoesNotExist(t, "t1")

			time.Sleep(tableTransitionExpiration)
			time.Sleep(2 * gcCheckInterval)
			// We're now both beyond table's timestamp as well as a tableGC interval
			validateTableDoesNotExist(t, tableName)
		})
	}
}

func TestPurge(t *testing.T) {
	for _, newNameFormat := range []bool{false, true} {
		t.Run(fmt.Sprintf("new format=%t", newNameFormat), func(t *testing.T) {
			populateTable(t)
			query, tableName, err := generateRenameStatement(newNameFormat, "t1", schema.PurgeTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
			require.NoError(t, err)

			_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
			require.NoError(t, err)

			validateTableDoesNotExist(t, "t1")
			if !fastDropTable {
				validateTableExists(t, tableName)
				checkTableRows(t, tableName, 1024)
			}
			if !fastDropTable {
				time.Sleep(5 * gcPurgeCheckInterval) // wait for table to be purged
			}
			validateTableDoesNotExist(t, tableName) // whether purged or not, table should at some point transition to next state
			if fastDropTable {
				// if MySQL supports fast DROP TABLE, TableGC completely skips the PURGE state. Rows are not purged.
				validateAnyState(t, 1024, schema.DropTableGCState, schema.TableDroppedGCState)
			} else {
				validateAnyState(t, 0, schema.EvacTableGCState, schema.DropTableGCState, schema.TableDroppedGCState)
			}
		})
	}
}

func TestPurgeView(t *testing.T) {
	populateTable(t)
	query, tableName, err := generateRenameStatement(true, "v1", schema.PurgeTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
	require.NoError(t, err)

	_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	require.NoError(t, err)

	// table untouched
	validateTableExists(t, "t1")
	if !fastDropTable {
		validateTableExists(t, tableName)
	}
	validateTableDoesNotExist(t, "v1")

	time.Sleep(tableTransitionExpiration / 2)
	{
		// View was created with +10s timestamp, so it should still exist
		if fastDropTable {
			// PURGE is skipped in mysql 8.0.23
			validateTableDoesNotExist(t, tableName)
		} else {
			validateTableExists(t, tableName)
			// We're really reading the view here:
			checkTableRows(t, tableName, 1024)
		}
	}

	time.Sleep(2 * gcPurgeCheckInterval) // wwait for table to be purged
	time.Sleep(2 * gcCheckInterval)      // wait for GC state transition

	// We're now both beyond view's timestamp as well as a tableGC interval
	validateTableDoesNotExist(t, tableName)
	// table still untouched
	validateTableExists(t, "t1")
	validateAnyState(t, 1024, schema.EvacTableGCState, schema.DropTableGCState, schema.TableDroppedGCState)
}

func TestDropView(t *testing.T) {
	viewName, err := schema.GenerateGCTableName(schema.DropTableGCState, time.Now().Add(tableTransitionExpiration)) // shortly in the future
	require.NoError(t, err)
	createStatement := fmt.Sprintf("create or replace view %s as select 1", viewName)

	_, err = primaryTablet.VttabletProcess.QueryTablet(createStatement, keyspaceName, true)
	require.NoError(t, err)

	// view should be there, because the timestamp hint is still in the near future.
	validateTableExists(t, viewName)

	time.Sleep(tableTransitionExpiration / 2)
	// But by now, after the above sleep, the view's timestamp hint is in the past, and we expect TableGC to have dropped the view.
	validateTableDoesNotExist(t, viewName)
}
