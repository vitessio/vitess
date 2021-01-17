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
package master

import (
	"flag"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	masterTablet    cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
	create table if not exists t1(
		id bigint not null auto_increment,
		value varchar(32),
		primary key(id)
	) Engine=InnoDB;
`

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
			"-lock_tables_timeout", "5s",
			"-watch_replication_stream",
			"-enable_replication_reporter",
			"-heartbeat_enable",
			"-heartbeat_interval", "250ms",
			"-gc_check_interval", "5s",
			"-table_gc_lifecycle", "hold,purge,evac,drop",
		}
		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

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
			if tablet.Type == "master" {
				masterTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
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
	rs, err := masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	assert.NoError(t, err)
	count := rs.Named().Row().AsInt64("c", 0)
	assert.Equal(t, expect, count)
}

func populateTable(t *testing.T) {
	_, err := masterTablet.VttabletProcess.QueryTablet(sqlSchema, keyspaceName, true)
	assert.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet("delete from t1", keyspaceName, true)
	assert.NoError(t, err)
	_, err = masterTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) values (null, md5(rand()))", keyspaceName, true)
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err = masterTablet.VttabletProcess.QueryTablet("insert into t1 (id, value) select null, md5(rand()) from t1", keyspaceName, true)
		assert.NoError(t, err)
	}
	checkTableRows(t, "t1", 1024)
}

// tableExists sees that a given table exists in MySQL
func tableExists(tableExpr string) (exists bool, tableName string, err error) {
	query := `show table status like '%a'`
	parsed := sqlparser.BuildParsedQuery(query, tableExpr)
	rs, err := masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	if err != nil {
		return false, "", err
	}
	row := rs.Named().Row()
	if row == nil {
		return false, "", nil
	}
	return true, row.AsString("Name", ""), nil
}

// tableExists sees that a given table exists in MySQL
func dropTable(tableName string) (err error) {
	query := `drop table if exists %a`
	parsed := sqlparser.BuildParsedQuery(query, tableName)
	_, err = masterTablet.VttabletProcess.QueryTablet(parsed.Query, keyspaceName, true)
	return err
}

func TestPopulateTable(t *testing.T) {
	populateTable(t)
	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.True(t, exists)
	}
	{
		exists, _, err := tableExists("no_such_table")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestHold(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.HoldTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	time.Sleep(5 * time.Second)
	{
		// Table was created with +10s timestamp, so it should still exist
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)

		checkTableRows(t, tableName, 1024)
	}

	time.Sleep(10 * time.Second)
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		// Table should be renamed as _vt_PURGE_...
		exists, purgeTableName, err := tableExists(`\_vt\_PURGE\_%`)
		assert.NoError(t, err)
		assert.True(t, exists)
		err = dropTable(purgeTableName)
		assert.NoError(t, err)
	}
}

func TestEvac(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.EvacTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	time.Sleep(5 * time.Second)
	{
		// Table was created with +10s timestamp, so it should still exist
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)

		checkTableRows(t, tableName, 1024)
	}

	time.Sleep(10 * time.Second)
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	time.Sleep(5 * time.Second)
	{
		// Table should be renamed as _vt_DROP_... and then dropped!
		exists, _, err := tableExists(`\_vt\_DROP\_%`)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestDrop(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.DropTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}

	time.Sleep(20 * time.Second) // 10s for timestamp to pass, then 10s for checkTables and drop of table
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
}

func TestPurge(t *testing.T) {
	populateTable(t)
	query, tableName, err := schema.GenerateRenameStatement("t1", schema.PurgeTableGCState, time.Now().UTC().Add(10*time.Second))
	assert.NoError(t, err)

	_, err = masterTablet.VttabletProcess.QueryTablet(query, keyspaceName, true)
	assert.NoError(t, err)

	{
		exists, _, err := tableExists("t1")
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	time.Sleep(5 * time.Second)
	{
		// Table was created with +10s timestamp, so it should still exist
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.True(t, exists)

		checkTableRows(t, tableName, 1024)
	}

	time.Sleep(1 * time.Minute) // purgeReentraceInterval
	{
		// We're now both beyond table's timestamp as well as a tableGC interval
		exists, _, err := tableExists(tableName)
		assert.NoError(t, err)
		assert.False(t, exists)
	}
	{
		// Table should be renamed as _vt_EVAC_...
		exists, evacTableName, err := tableExists(`\_vt\_EVAC\_%`)
		assert.NoError(t, err)
		assert.True(t, exists)
		checkTableRows(t, evacTableName, 0)
		err = dropTable(evacTableName)
		assert.NoError(t, err)
	}
}
