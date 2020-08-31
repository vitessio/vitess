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

package onlineddl

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance       *cluster.LocalProcessCluster
	hostname              = "localhost"
	keyspaceName          = "ks"
	cell                  = "zone1"
	schemaChangeDirectory = ""
	totalTableCount       = 4
	createTable           = `
		CREATE TABLE %s (
		id BIGINT(20) not NULL,
		msg varchar(64),
		PRIMARY KEY (id)
		) ENGINE=InnoDB;`
	alterTable = `
		ALTER WITH 'gh-ost' TABLE %s
		ADD COLUMN new_id bigint(20) NOT NULL AUTO_INCREMENT FIRST,
		DROP PRIMARY KEY,
		ADD PRIMARY KEY (new_id),
		ADD INDEX idx_column(%s)`
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

		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name: keyspaceName,
		}

		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 2, true); err != nil {
			return 1, err
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"1"}, 1, false); err != nil {
			return 1, err
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
	testWithInitialSchema(t)
	testWithAlterSchema(t)
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	var sqlQuery = "" //nolint
	for i := 0; i < totalTableCount; i++ {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_onlineddl_test_%02d", i))
		err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
		require.Nil(t, err)
	}

	// Check if 4 tables are created
	checkTables(t, totalTableCount)
	checkTables(t, totalTableCount)

	// Also match the vschema for those tablets
	matchSchema(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0].VttabletProcess.TabletPath)
}

// testWithAlterSchema if we alter schema and then apply, the resultant schema should match across shards
func testWithAlterSchema(t *testing.T) {
	tableName := fmt.Sprintf("vt_onlineddl_test_%02d", 3)
	sqlQuery := fmt.Sprintf(alterTable, tableName, "msg")
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
	require.Nil(t, err)
	// Migration is asynchronous. Give it some time.
	time.Sleep(time.Second * 30)
	matchSchema(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0].VttabletProcess.TabletPath)
	checkMigratedTable(t, tableName)
}

// matchSchema schema for supplied tablets should match
func matchSchema(t *testing.T, firstTablet string, secondTablet string) {
	firstShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", firstTablet)
	require.Nil(t, err)

	secondShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", secondTablet)
	require.Nil(t, err)

	assert.Equal(t, firstShardSchema, secondShardSchema)
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, count int) {
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0], count)
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0], count)
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *cluster.Vttablet, count int) {
	queryResult, err := tablet.VttabletProcess.QueryTablet("show tables;", keyspaceName, true)
	require.Nil(t, err)
	assert.Equal(t, len(queryResult.Rows), count)
}

// checkMigratedTables checks the CREATE STATEMENT of a table after migration
func checkMigratedTable(t *testing.T, tableName string) {
	expect := "new_id"
	checkTableCreateContains(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0], tableName, expect)
}

// checkTableCreateContains checks if table's CREATE TABLE statement contains a given test
func checkTableCreateContains(t *testing.T, tablet *cluster.Vttablet, tableName string, expect string) {
	queryResult, err := tablet.VttabletProcess.QueryTablet(fmt.Sprintf("show create table %s;", tableName), keyspaceName, true)
	require.Nil(t, err)

	assert.Equal(t, len(queryResult.Rows), 1)
	assert.Equal(t, len(queryResult.Rows[0]), 2) // table name, create statement
	createStatement := queryResult.Rows[0][1].ToString()
	assert.True(t, strings.Contains(createStatement, expect))
}
