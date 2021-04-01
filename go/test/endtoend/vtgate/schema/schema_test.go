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

package schema

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
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
		ALTER TABLE %s
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
	testWithAlterDatabase(t)
	testWithDropCreateSchema(t)
	testSchemaChangePreflightErrorPartially(t)
	testDropNonExistentTables(t)
	testCopySchemaShards(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, 2)
	testCopySchemaShards(t, fmt.Sprintf("%s/0", keyspaceName), 3)
	testCopySchemaShardWithDifferentDB(t, 4)
	testWithAutoSchemaFromChangeDir(t)
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	var sqlQuery = "" //nolint
	for i := 0; i < totalTableCount; i++ {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", i))
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
	sqlQuery := fmt.Sprintf(alterTable, fmt.Sprintf("vt_select_test_%02d", 3), "msg")
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sqlQuery)
	require.Nil(t, err)
	matchSchema(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0].VttabletProcess.TabletPath)
}

// testWithAlterDatabase tests that ALTER DATABASE is accepted by the validator.
func testWithAlterDatabase(t *testing.T) {
	sql := "create database alter_database_test; alter database alter_database_test default character set = utf8mb4; drop database alter_database_test"
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, sql)
	assert.Nil(t, err)
}

// testWithDropCreateSchema , we should be able to drop and create same schema
//Tests that a DROP and CREATE table will pass PreflightSchema check.
//
//PreflightSchema checks each SQL statement separately. When doing so, it must
//consider previous statements within the same ApplySchema command. For
//example, a CREATE after DROP must not fail: When CREATE is checked, DROP
//must have been executed first.
//See: https://github.com/vitessio/vitess/issues/1731#issuecomment-222914389
func testWithDropCreateSchema(t *testing.T) {
	dropCreateTable := fmt.Sprintf("DROP TABLE vt_select_test_%02d ;", 2) + fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", 2))
	err := clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, dropCreateTable)
	require.Nil(t, err)
	checkTables(t, totalTableCount)
}

// testWithAutoSchemaFromChangeDir on putting sql file to schema change directory, it should apply that sql to all shards
func testWithAutoSchemaFromChangeDir(t *testing.T) {
	_ = os.Mkdir(path.Join(schemaChangeDirectory, keyspaceName), 0700)
	_ = os.Mkdir(path.Join(schemaChangeDirectory, keyspaceName, "input"), 0700)
	sqlFile := path.Join(schemaChangeDirectory, keyspaceName, "input/create_test_table_x.sql")
	err := ioutil.WriteFile(sqlFile, []byte("create table test_table_x (id int)"), 0644)
	require.Nil(t, err)
	timeout := time.Now().Add(10 * time.Second)
	matchFoundAfterAutoSchemaApply := false
	for time.Now().Before(timeout) {
		if _, err := os.Stat(sqlFile); os.IsNotExist(err) {
			matchFoundAfterAutoSchemaApply = true
			checkTables(t, totalTableCount+1)
			matchSchema(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, clusterInstance.Keyspaces[0].Shards[1].Vttablets[0].VttabletProcess.TabletPath)
		}
	}
	if !matchFoundAfterAutoSchemaApply {
		assert.Fail(t, "Auto schema is not consumed")
	}
	defer os.RemoveAll(path.Join(schemaChangeDirectory, keyspaceName))
}

// matchSchema schema for supplied tablets should match
func matchSchema(t *testing.T, firstTablet string, secondTablet string) {
	firstShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", firstTablet)
	require.Nil(t, err)

	secondShardSchema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", secondTablet)
	require.Nil(t, err)

	assert.Equal(t, firstShardSchema, secondShardSchema)
}

// testSchemaChangePreflightErrorPartially applying same schema + new schema should throw error for existing one
// Tests that some SQL statements fail properly during PreflightSchema.
func testSchemaChangePreflightErrorPartially(t *testing.T) {
	createNewTable := fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", 5)) + fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", 2))
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ApplySchema", "-sql", createNewTable, keyspaceName)
	require.Error(t, err)
	assert.True(t, strings.Contains(output, "already exists"))

	checkTables(t, totalTableCount)
}

// testDropNonExistentTables applying same schema + new schema should throw error for existing one and also add the new schema
//If a table does not exist, DROP TABLE should error during preflight
//because the statement does not change the schema as there is
//nothing to drop.
//In case of DROP TABLE IF EXISTS though, it should not error as this
//is the MySQL behavior the user expects.
func testDropNonExistentTables(t *testing.T) {
	dropNonExistentTable := "DROP TABLE nonexistent_table;"
	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ApplySchema", "-sql", dropNonExistentTable, keyspaceName)
	require.Error(t, err)
	assert.True(t, strings.Contains(output, "Unknown table"))

	dropIfExists := "DROP TABLE IF EXISTS nonexistent_table;"
	err = clusterInstance.VtctlclientProcess.ApplySchema(keyspaceName, dropIfExists)
	require.Nil(t, err)

	checkTables(t, totalTableCount)
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

// testCopySchemaShards tests that schema from source is correctly applied to destination
func testCopySchemaShards(t *testing.T, source string, shard int) {
	addNewShard(t, shard)
	// InitShardMaster creates the db, but there shouldn't be any tables yet.
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0], 0)
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[1], 0)
	// Run the command twice to make sure it's idempotent.
	for i := 0; i < 2; i++ {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("CopySchemaShard", source, fmt.Sprintf("%s/%d", keyspaceName, shard))
		require.Nil(t, err)
	}
	// shard_2_master should look the same as the replica we copied from
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0], totalTableCount)
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[1], totalTableCount)

	matchSchema(t, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.TabletPath, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0].VttabletProcess.TabletPath)
}

// testCopySchemaShardWithDifferentDB if we apply different schema to new shard, it should throw error
func testCopySchemaShardWithDifferentDB(t *testing.T, shard int) {
	addNewShard(t, shard)
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0], 0)
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[1], 0)
	source := fmt.Sprintf("%s/0", keyspaceName)

	masterTabletAlias := clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0].VttabletProcess.TabletPath
	schema, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", masterTabletAlias)
	require.Nil(t, err)

	resultMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(schema), &resultMap)
	require.Nil(t, err)
	dbSchema := reflect.ValueOf(resultMap["database_schema"])
	assert.True(t, strings.Contains(dbSchema.String(), "utf8"))

	// Change the db charset on the destination shard from utf8 to latin1.
	// This will make CopySchemaShard fail during its final diff.
	// (The different charset won't be corrected on the destination shard
	//  because we use "CREATE DATABASE IF NOT EXISTS" and this doesn't fail if
	//  there are differences in the options e.g. the character set.)
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ExecuteFetchAsDba", "-json", masterTabletAlias, "ALTER DATABASE vt_ks CHARACTER SET latin1")
	require.Nil(t, err)

	output, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("CopySchemaShard", source, fmt.Sprintf("%s/%d", keyspaceName, shard))
	require.Error(t, err)
	assert.True(t, strings.Contains(output, "schemas are different"))

	// shard_2_master should have the same number of tables. Only the db
	// character set is different.
	checkTablesCount(t, clusterInstance.Keyspaces[0].Shards[shard].Vttablets[0], totalTableCount)
}

// addNewShard adds a new shard dynamically
func addNewShard(t *testing.T, shard int) {
	keyspace := &cluster.Keyspace{
		Name: keyspaceName,
	}
	err := clusterInstance.StartKeyspace(*keyspace, []string{fmt.Sprintf("%d", shard)}, 1, false)
	require.Nil(t, err)
}
