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
	"os"
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
		create or replace view v1 as select * from t1;
	`
	sqlSchema = sqlCreateTable + sqlCreateView

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
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--gc_check_interval", gcCheckInterval.String(),
			"--gc_purge_check_interval", gcPurgeCheckInterval.String(),
			"--table_gc_lifecycle", "hold,purge,evac,drop",
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
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

func checkTableRows(t *testing.T, tableName string, expect int64) {
	require.NotEmpty(t, tableName)
	query := `select count(*) as c from %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	rs, err := primaryTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	require.NoError(t, err)
	count := rs.Named().Row().AsInt64("c", 0)
	assert.Equal(t, expect, count)
}

func populateTable(t *testing.T) {
	_, err := primaryTablet.VttabletProcess.QueryTablet(sqlSchema, keyspaceName, true)
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
func tableExists(tableExpr string) (exists bool, tableName string, err error) {
	query := `select table_name as table_name from information_schema.tables where table_schema=database() and table_name like '%a'`
	parsed := sqlparser.BuildParsedQuery(query, tableExpr)
	rs, err := primaryTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
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
	var foundTableName string
	var exists bool
	var err error
	for {
		select {
		case <-ticker.C:
			exists, foundTableName, err = tableExists(tableExpr)
			require.NoError(t, err)
			if !exists {
				return
			}
		case <-ctx.Done():
			assert.NoError(t, ctx.Err(), "validateTableDoesNotExist timed out, table %v still exists (%v)", tableExpr, foundTableName)
			return
		}
	}
}

func validateTableExists(t *testing.T, tableExpr string) {
	ctx, cancel := context.WithTimeout(context.Background(), waitForTransitionTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	var exists bool
	var err error
	for {
		select {
		case <-ticker.C:
			exists, _, err = tableExists(tableExpr)
			require.NoError(t, err)
			if exists {
				return
			}
		case <-ctx.Done():
			assert.NoError(t, ctx.Err(), "validateTableExists timed out, table %v still does not exist", tableExpr)
			return
		}
	}
}

func validateAnyState(t *testing.T, expectNumRows int64, states ...schema.TableGCState) {
	for _, state := range states {
		expectTableToExist := true
		searchExpr := ""
		switch state {
		case schema.HoldTableGCState:
			searchExpr = `\_vt\_HOLD\_%`
		case schema.PurgeTableGCState:
			searchExpr = `\_vt\_PURGE\_%`
		case schema.EvacTableGCState:
			searchExpr = `\_vt\_EVAC\_%`
		case schema.DropTableGCState:
			searchExpr = `\_vt\_DROP\_%`
		case schema.TableDroppedGCState:
			searchExpr = `\_vt\_%`
			expectTableToExist = false
		default:
			t.Log("Unknown state")
			t.Fail()
		}
		exists, tableName, err := tableExists(searchExpr)
		require.NoError(t, err)

		if exists {
			if expectNumRows >= 0 {
				checkTableRows(t, tableName, expectNumRows)
			}
			// Now that the table is validated, we can drop it
			dropTable(t, tableName)
		}
		if exists == expectTableToExist {
			// condition met
			return
		}
	}
	assert.Failf(t, "could not match any of the states", "states=%v", states)
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

	_, capableOf, _ := mysql.GetFlavor(mysqlVersion, nil)
	require.NotNil(t, capableOf)
	var err error
	fastDropTable, err = capableOf(mysql.FastDropTableFlavorCapability)
	require.NoError(t, err)
}

func TestPopulateTable(t *testing.T) {
	populateTable(t)
	validateTableExists(t, "t1")
	validateTableDoesNotExist(t, "no_such_table")
}

func TestHold(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.HoldTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
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
}

func TestEvac(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.EvacTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
	assert.NoError(t, err)

	_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	validateTableDoesNotExist(t, "t1")

	time.Sleep(tableTransitionExpiration / 2)
	{
		// Table was created with +10s timestamp, so it should still exist
		if fastDropTable {
			// EVAC state is skipped in mysql 8.0.23 and beyond
			validateTableDoesNotExist(t, tableName)
		} else {
			validateTableExists(t, tableName)
			checkTableRows(t, tableName, 1024)
		}
	}

	time.Sleep(tableTransitionExpiration)
	// We're now both beyond table's timestamp as well as a tableGC interval
	validateTableDoesNotExist(t, tableName)
	// Table should be renamed as _vt_DROP_... and then dropped!
	validateAnyState(t, 0, schema.DropTableGCState, schema.TableDroppedGCState)
}

func TestDrop(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.DropTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
	assert.NoError(t, err)

	_, err = primaryTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	validateTableDoesNotExist(t, "t1")

	time.Sleep(tableTransitionExpiration)
	time.Sleep(2 * gcCheckInterval)
	// We're now both beyond table's timestamp as well as a tableGC interval
	validateTableDoesNotExist(t, tableName)
}

func TestPurge(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.PurgeTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
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
}

func TestPurgeView(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("v1", schema.PurgeTableGCState, time.Now().UTC().Add(tableTransitionExpiration))
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
