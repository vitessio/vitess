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
	"context"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
)

var (
	clusterInstance       *vitesst.Cluster
	keyspaceName          = "ks"
	schemaChangeDirectory = "/vt/vtdataroot/schema_change_dir"
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

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithoutVTGate(),
		vitesst.WithVTCtldArgs(
			"--schema-change-dir", schemaChangeDirectory,
			"--schema-change-controller", "local",
			"--schema-change-check-interval", "1s",
		),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames("0", "1").
			WithReplicas(1),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
}

func TestSchemaChange(t *testing.T) {
	setup(t)
	testWithInitialSchema(t)
	testWithAlterSchema(t)
	testWithAlterDatabase(t)
	testWithDropCreateSchema(t)
	testDropNonExistentTables(t)
	testApplySchemaBatch(t)
	testUnsafeAllowForeignKeys(t)
	testCreateInvalidView(t)
	testCopySchemaShards(t, shardTablet(t, "0", 0).Alias(), 2)
	testCopySchemaShards(t, keyspaceName+"/0", 3)
	testCopySchemaShardWithDifferentDB(t, 4)
	testWithAutoSchemaFromChangeDir(t)
}

// shardTablet returns the tablet of the given shard at the given index, with
// the shard's primary first.
func shardTablet(t *testing.T, shardName string, index int) *vitesst.Tablet {
	keyspace := clusterInstance.Keyspace(keyspaceName)
	require.NotNil(t, keyspace)

	shard := keyspace.Shard(shardName)
	require.NotNil(t, shard)

	tablets := shard.Tablets()
	require.Greater(t, len(tablets), index)
	return tablets[index]
}

// applySchema applies DDL to the keyspace with the direct strategy.
func applySchema(t *testing.T, sql string) error {
	return clusterInstance.Vtctld().ExecuteCommand(
		t.Context(),
		"ApplySchema",
		"--sql", sql,
		"--ddl-strategy", "direct -allow-zero-in-date",
		keyspaceName,
	)
}

func testWithInitialSchema(t *testing.T) {
	// Create 4 tables
	var sqlQuery string
	for i := range totalTableCount {
		sqlQuery = fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", i))
		err := applySchema(t, sqlQuery)
		require.Nil(t, err)
	}

	// Check if 4 tables are created
	checkTables(t, totalTableCount)

	// Also match the vschema for those tablets
	matchSchema(t, shardTablet(t, "0", 0).Alias(), shardTablet(t, "1", 0).Alias())
}

// testWithAlterSchema if we alter schema and then apply, the resultant schema should match across shards
func testWithAlterSchema(t *testing.T) {
	sqlQuery := fmt.Sprintf(alterTable, fmt.Sprintf("vt_select_test_%02d", 3), "msg")
	err := applySchema(t, sqlQuery)
	require.Nil(t, err)
	matchSchema(t, shardTablet(t, "0", 0).Alias(), shardTablet(t, "1", 0).Alias())
}

// testWithAlterDatabase tests that ALTER DATABASE is accepted by the validator.
func testWithAlterDatabase(t *testing.T) {
	sql := "create database alter_database_test; alter database alter_database_test default character set = utf8mb4; drop database alter_database_test"
	err := applySchema(t, sql)
	assert.NoError(t, err)
}

// testWithDropCreateSchema , we should be able to drop and create same schema
// Tests that a DROP and CREATE table will pass PreflightSchema check.
//
// PreflightSchema checks each SQL statement separately. When doing so, it must
// consider previous statements within the same ApplySchema command. For
// example, a CREATE after DROP must not fail: When CREATE is checked, DROP
// must have been executed first.
// See: https://github.com/vitessio/vitess/issues/1731#issuecomment-222914389
func testWithDropCreateSchema(t *testing.T) {
	dropCreateTable := fmt.Sprintf("DROP TABLE vt_select_test_%02d ;", 2) + fmt.Sprintf(createTable, fmt.Sprintf("vt_select_test_%02d", 2))
	err := applySchema(t, dropCreateTable)
	require.NoError(t, err)
	checkTables(t, totalTableCount)
}

// testWithAutoSchemaFromChangeDir on putting sql file to schema change directory, it should apply that sql to all shards
func testWithAutoSchemaFromChangeDir(t *testing.T) {
	ctx := t.Context()
	vtctld := clusterInstance.Vtctld()

	keyspaceDir := path.Join(schemaChangeDirectory, keyspaceName)
	exitCode, output, err := vtctld.Exec(ctx, "mkdir", "-p", path.Join(keyspaceDir, "input"))
	require.Nil(t, err)
	require.Zero(t, exitCode, output)

	sqlFile := path.Join(keyspaceDir, "input/create_test_table_x.sql")
	err = vtctld.WriteFile(ctx, sqlFile, "create table test_table_x (id int)")
	require.Nil(t, err)
	timeout := time.Now().Add(10 * time.Second)
	matchFoundAfterAutoSchemaApply := false
	for time.Now().Before(timeout) {
		exitCode, _, err := vtctld.Exec(ctx, "test", "-f", sqlFile)
		require.Nil(t, err)
		if exitCode != 0 {
			matchFoundAfterAutoSchemaApply = true
			checkTables(t, totalTableCount+1)
			matchSchema(t, shardTablet(t, "0", 0).Alias(), shardTablet(t, "1", 0).Alias())
			break
		}
	}
	assert.True(t, matchFoundAfterAutoSchemaApply, "Auto schema is not consumed")
	defer func() {
		require.Nil(t, vtctld.RemoveFile(ctx, keyspaceDir))
	}()
}

// matchSchema schema for supplied tablets should match
func matchSchema(t *testing.T, firstTablet string, secondTablet string) {
	firstShardSchema, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetSchema", firstTablet)
	require.Nil(t, err)

	secondShardSchema, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetSchema", secondTablet)
	require.Nil(t, err)

	assert.Equal(t, firstShardSchema, secondShardSchema)
}

// testDropNonExistentTables applying same schema + new schema should throw error for existing one and also add the new schema
// If a table does not exist, DROP TABLE should error during preflight
// because the statement does not change the schema as there is
// nothing to drop.
// In case of DROP TABLE IF EXISTS though, it should not error as this
// is the MySQL behavior the user expects.
func testDropNonExistentTables(t *testing.T) {
	dropNonExistentTable := "DROP TABLE nonexistent_table;"
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--sql", dropNonExistentTable, keyspaceName)
	require.Error(t, err)
	assert.True(t, strings.Contains(output, "Unknown table"))

	dropIfExists := "DROP TABLE IF EXISTS nonexistent_table;"
	err = applySchema(t, dropIfExists)
	require.Nil(t, err)

	checkTables(t, totalTableCount)
}

// testCreateInvalidView attempts to create a view that depends on non-existent table. We expect an error
// we test with different 'direct' strategy options
func testCreateInvalidView(t *testing.T) {
	for _, ddlStrategy := range []string{"direct", "direct -allow-zero-in-date"} {
		createInvalidView := "CREATE OR REPLACE VIEW invalid_view AS SELECT * FROM nonexistent_table;"
		output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--ddl-strategy", ddlStrategy, "--sql", createInvalidView, keyspaceName)
		require.Error(t, err)
		assert.Contains(t, output, "doesn't exist (errno 1146)")
	}
}

func testApplySchemaBatch(t *testing.T) {
	{
		sqls := "create table batch1(id int primary key);create table batch2(id int primary key);create table batch3(id int primary key);create table batch4(id int primary key);create table batch5(id int primary key);"
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--sql", sqls, "--batch-size", "2", keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount+5)
	}
	{
		sqls := "drop table batch1; drop table batch2; drop table batch3; drop table batch4; drop table batch5"
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--sql", sqls, keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount)
	}
	{
		sqls := "create table batch1(id int primary key);create table batch2(id int primary key);create table batch3(id int primary key);create table batch4(id int primary key);create table batch5(id int primary key);"
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--ddl-strategy", "direct --allow-zero-in-date", "--sql", sqls, "--batch-size", "2", keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount+5)
	}
	{
		sqls := "drop table batch1; drop table batch2; drop table batch3; drop table batch4; drop table batch5"
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--sql", sqls, keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount)
	}
}

func testUnsafeAllowForeignKeys(t *testing.T) {
	sqls := `
		create table t11 (id int primary key, i int, constraint f1101 foreign key (i) references t12 (id) on delete restrict);
		create table t12 (id int primary key, i int, constraint f1201 foreign key (i) references t11 (id) on delete set null);
	`
	{
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--ddl-strategy", "direct --allow-zero-in-date", "--sql", sqls, keyspaceName)
		assert.Error(t, err)
		checkTables(t, totalTableCount)
	}
	{
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--ddl-strategy", "direct --unsafe-allow-foreign-keys --allow-zero-in-date", "--sql", sqls, keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount+2)
	}
	{
		_, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "ApplySchema", "--sql", "drop table t11, t12", keyspaceName)
		require.NoError(t, err)
		checkTables(t, totalTableCount)
	}
}

// checkTables checks the number of tables in the first two shards.
func checkTables(t *testing.T, count int) {
	checkTablesCount(t, shardTablet(t, "0", 0), count)
	checkTablesCount(t, shardTablet(t, "1", 0), count)
}

// checkTablesCount checks the number of tables in the given tablet
func checkTablesCount(t *testing.T, tablet *vitesst.Tablet, count int) {
	queryResult, err := tablet.QueryTablet(t.Context(), "show tables;")
	require.Nil(t, err)
	assert.Equal(t, len(queryResult.Rows), count)
}

// testCopySchemaShards tests that schema from source is correctly applied to destination
func testCopySchemaShards(t *testing.T, source string, shard int) {
	addNewShard(t, shard)
	shardName := strconv.Itoa(shard)
	// The primary election creates the db, but there shouldn't be any tables yet.
	checkTablesCount(t, shardTablet(t, shardName, 0), 0)
	checkTablesCount(t, shardTablet(t, shardName, 1), 0)
	// Run the command twice to make sure it's idempotent.
	for range 2 {
		err := clusterInstance.Vtctld().ExecuteCommand(t.Context(), "CopySchemaShard", source, fmt.Sprintf("%s/%d", keyspaceName, shard))
		require.Nil(t, err)
	}
	// shard2 primary should look the same as the replica we copied from
	checkTablesCount(t, shardTablet(t, shardName, 0), totalTableCount)
	checkTablesCount(t, shardTablet(t, shardName, 1), totalTableCount)

	matchSchema(t, shardTablet(t, "0", 0).Alias(), shardTablet(t, shardName, 0).Alias())
}

// testCopySchemaShardWithDifferentDB if we apply different schema to new shard, it should throw error
func testCopySchemaShardWithDifferentDB(t *testing.T, shard int) {
	addNewShard(t, shard)
	shardName := strconv.Itoa(shard)
	checkTablesCount(t, shardTablet(t, shardName, 0), 0)
	checkTablesCount(t, shardTablet(t, shardName, 1), 0)
	source := keyspaceName + "/0"

	tabletAlias := shardTablet(t, shardName, 0).Alias()
	schema, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetSchema", tabletAlias)
	require.Nil(t, err)

	resultMap := make(map[string]any)
	err = json.Unmarshal([]byte(schema), &resultMap)
	require.Nil(t, err)
	dbSchema := reflect.ValueOf(resultMap["database_schema"])
	assert.True(t, strings.Contains(dbSchema.String(), "utf8"))

	// Change the db charset on the destination shard from utf8 to latin1.
	// This will make CopySchemaShard fail during its final diff.
	// (The different charset won't be corrected on the destination shard
	//  because we use "CREATE DATABASE IF NOT EXISTS" and this doesn't fail if
	//  there are differences in the options e.g. the character set.)
	err = clusterInstance.Vtctld().ExecuteCommand(t.Context(), "ExecuteFetchAsDBA", "--json", tabletAlias, "ALTER DATABASE vt_ks CHARACTER SET latin1")
	require.Nil(t, err)

	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "CopySchemaShard", source, fmt.Sprintf("%s/%d", keyspaceName, shard))
	require.Error(t, err)
	assert.True(t, strings.Contains(output, "schemas are different"))

	// shard2 primary should have the same number of tables. Only the db
	// character set is different.
	checkTablesCount(t, shardTablet(t, shardName, 0), totalTableCount)
}

// addNewShard adds a new shard dynamically
func addNewShard(t *testing.T, shard int) {
	_, err := clusterInstance.AddShard(t, t.Context(), keyspaceName, strconv.Itoa(shard), 1, 0)
	require.Nil(t, err)
}
