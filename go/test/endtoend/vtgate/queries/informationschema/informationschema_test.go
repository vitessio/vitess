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

package informationschema

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T) (*mysql.Conn, *mysql.Conn, func()) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.Nil(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, vtConn, "set workload = oltp")

		tables := []string{
			"t1", "t1_id2_idx", "vstream_test", "t2", "t2_id4_idx", "t3", "t3_id7_idx", "t4",
			"t4_id2_idx", "t5_null_vindex", "t6", "t6_id2_idx", "t7_xxhash", "t7_xxhash_idx", "t7_fk", "t8",
		}
		conns := []*mysql.Conn{vtConn, mysqlConn}
		for _, conn := range conns {
			for _, table := range tables {
				_, _ = utils.ExecAllowError(t, conn, "delete from "+table)
			}
		}
	}

	deleteAll()

	return vtConn, mysqlConn, func() {
		deleteAll()
		vtConn.Close()
		mysqlConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestDbNameOverride(t *testing.T) {
	vtConn, _, closer := start(t)
	defer closer()

	qr, err := vtConn.ExecuteFetch("SELECT distinct database() FROM information_schema.tables WHERE table_schema = database()", 1000, true)

	require.Nil(t, err)
	assert.Equal(t, 1, len(qr.Rows), "did not get enough rows back")
	assert.Equal(t, "vt_ks", qr.Rows[0][0].ToString())
}

func TestInformationSchemaQuery(t *testing.T) {
	vtConn, _, closer := start(t)
	defer closer()

	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'ks'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'vt_ks'", "vt_ks")
	utils.AssertResultIsEmpty(t, vtConn, "table_schema = 'NONE'")
	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'performance_schema'", "performance_schema")
	utils.AssertResultIsEmpty(t, vtConn, "table_schema = 'PERFORMANCE_SCHEMA'")
	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'performance_schema' and table_name = 'users'", "performance_schema")
	utils.AssertResultIsEmpty(t, vtConn, "table_schema = 'performance_schema' and table_name = 'foo'")
	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'vt_ks' and table_name = 't1'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, vtConn, "table_schema = 'ks' and table_name = 't1'", "vt_ks")
}

func TestInformationSchemaWithSubquery(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.AssertIsEmptyCompareMySQL(t, vtConn, mysqlConn, "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = (SELECT SCHEMA()) AND TABLE_NAME = 'not_exists'")
}

func TestInformationSchemaQueryGetsRoutedToTheRightTableAndKeyspace(t *testing.T) {
	t.Skip("flaky. skipping for now")
	vtConn, _, closer := start(t)
	defer closer()

	utils.Exec(t, vtConn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")

	_ = utils.Exec(t, vtConn, "SELECT /*vt+ PLANNER=gen4 */ * FROM t1000") // test that the routed table is available to us
	result := utils.Exec(t, vtConn, "SELECT /*vt+ PLANNER=gen4 */ * FROM information_schema.tables WHERE table_schema = database() and table_name='t1000'")
	assert.NotEmpty(t, result.Rows)
}

func TestFKConstraintUsingInformationSchema(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	query := "select  fk.referenced_table_name as to_table, fk.referenced_column_name as primary_key, fk.column_name as `column`, fk.constraint_name as name, rc.update_rule as on_update, rc.delete_rule as on_delete from information_schema.referential_constraints as rc join information_schema.key_column_usage as fk on fk.constraint_schema = rc.constraint_schema and fk.constraint_name = rc.constraint_name where fk.referenced_column_name is not null and fk.table_schema = database() and fk.table_name = 't7_fk' and rc.constraint_schema = database() and rc.table_name = 't7_fk'"
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, query, `[[VARCHAR("t7_xxhash") VARCHAR("uid") VARCHAR("t7_uid") VARCHAR("t7_fk_ibfk_1") VARCHAR("CASCADE") VARCHAR("SET NULL")]]`)
}

func TestConnectWithSystemSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		vtConnParams := vtParams
		vtConnParams.DbName = dbname
		mysqlConnParams := mysqlParams
		mysqlConnParams.DbName = dbname

		vtConn, err := mysql.Connect(ctx, &vtConnParams)
		require.Nil(t, err)

		mysqlConn, err := mysql.Connect(ctx, &mysqlConnParams)
		require.Nil(t, err)
		defer func() {
			vtConn.Close()
			mysqlConn.Close()
		}()

		utils.ExecCompareMySQL(t, vtConn, mysqlConn, `select @@max_allowed_packet from dual`)
	}
}

func TestUseSystemSchema(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		utils.ExecCompareMySQL(t, vtConn, mysqlConn, fmt.Sprintf("use %s", dbname))
		utils.ExecCompareMySQL(t, vtConn, mysqlConn, `select @@max_allowed_packet from dual`)
	}
}

func TestSystemSchemaQueryWithoutQualifier(t *testing.T) {
	vtConn, _, closer := start(t)
	defer closer()

	queryWithQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", keyspaceName, keyspaceName)
	qr1 := utils.Exec(t, vtConn, queryWithQualifier)

	utils.Exec(t, vtConn, "use information_schema")
	queryWithoutQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from tables t "+
		"join columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", keyspaceName, keyspaceName)
	qr2 := utils.Exec(t, vtConn, queryWithoutQualifier)
	require.Equal(t, qr1, qr2)

	ctx := context.Background()
	connParams := vtParams
	connParams.DbName = "information_schema"
	conn2, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn2.Close()

	qr3 := utils.Exec(t, conn2, queryWithoutQualifier)
	require.Equal(t, qr2, qr3)
}

func TestMultipleSchemaPredicates(t *testing.T) {
	vtConn, _, closer := start(t)
	defer closer()

	query := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", keyspaceName, keyspaceName, keyspaceName, keyspaceName)
	qr1 := utils.Exec(t, vtConn, query)
	require.EqualValues(t, 4, len(qr1.Fields))

	// test a query with two keyspace names
	query = fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", keyspaceName, keyspaceName, "a")
	_, err := vtConn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "specifying two different database in the query is not supported")
}
