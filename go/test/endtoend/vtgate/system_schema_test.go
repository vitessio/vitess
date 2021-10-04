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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestDbNameOverride(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	qr, err := conn.ExecuteFetch("SELECT distinct database() FROM information_schema.tables WHERE table_schema = database()", 1000, true)

	require.Nil(t, err)
	assert.Equal(t, 1, len(qr.Rows), "did not get enough rows back")
	assert.Equal(t, "vt_ks", qr.Rows[0][0].ToString())
}

func TestInformationSchemaQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'ks'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'vt_ks'", "vt_ks")
	utils.AssertResultIsEmpty(t, conn, "table_schema = 'NONE'")
	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'performance_schema'", "performance_schema")
	utils.AssertResultIsEmpty(t, conn, "table_schema = 'PERFORMANCE_SCHEMA'")
	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'performance_schema' and table_name = 'users'", "performance_schema")
	utils.AssertResultIsEmpty(t, conn, "table_schema = 'performance_schema' and table_name = 'foo'")
	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'vt_ks' and table_name = 't1'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, conn, "table_schema = 'ks' and table_name = 't1'", "vt_ks")
}

func TestInformationSchemaWithSubquery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	result := utils.Exec(t, conn, "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = (SELECT SCHEMA()) AND TABLE_NAME = 'not_exists'")
	assert.Empty(t, result.Rows)
}

func TestInformationSchemaQueryGetsRoutedToTheRightTableAndKeyspace(t *testing.T) {
	t.Skip("flaky. skipping for now")
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, "delete from t1") // delete everything in t1 (routed to t1000)
	defer utils.Exec(t, conn, "delete from t1")

	utils.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")

	_ = utils.Exec(t, conn, "SELECT * FROM t1000") // test that the routed table is available to us
	result := utils.Exec(t, conn, "SELECT * FROM information_schema.tables WHERE table_schema = database() and table_name='t1000'")
	assert.NotEmpty(t, result.Rows)
}

func TestFKConstraintUsingInformationSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := "select fk.referenced_table_name as to_table, fk.referenced_column_name as primary_key, fk.column_name as `column`, fk.constraint_name as name, rc.update_rule as on_update, rc.delete_rule as on_delete from information_schema.referential_constraints as rc join information_schema.key_column_usage as fk using (constraint_schema, constraint_name) where fk.referenced_column_name is not null and fk.table_schema = database() and fk.table_name = 't7_fk' and rc.constraint_schema = database() and rc.table_name = 't7_fk'"
	utils.AssertMatches(t, conn, query, `[[VARCHAR("t7_xxhash") VARCHAR("uid") VARCHAR("t7_uid") VARCHAR("t7_fk_ibfk_1") VARCHAR("CASCADE") VARCHAR("SET NULL")]]`)
}

func TestConnectWithSystemSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		connParams := vtParams
		connParams.DbName = dbname
		conn, err := mysql.Connect(ctx, &connParams)
		require.NoError(t, err)
		utils.Exec(t, conn, `select @@max_allowed_packet from dual`)
		conn.Close()
	}
}

func TestUseSystemSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		utils.Exec(t, conn, fmt.Sprintf("use %s", dbname))
		utils.Exec(t, conn, `select @@max_allowed_packet from dual`)
	}
}

func TestSystemSchemaQueryWithoutQualifier(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	queryWithQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", KeyspaceName, KeyspaceName)
	qr1 := utils.Exec(t, conn, queryWithQualifier)

	utils.Exec(t, conn, "use information_schema")
	queryWithoutQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from tables t "+
		"join columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", KeyspaceName, KeyspaceName)
	qr2 := utils.Exec(t, conn, queryWithoutQualifier)
	require.Equal(t, qr1, qr2)

	connParams := vtParams
	connParams.DbName = "information_schema"
	conn2, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn2.Close()

	qr3 := utils.Exec(t, conn2, queryWithoutQualifier)
	require.Equal(t, qr2, qr3)
}

func TestMultipleSchemaPredicates(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", KeyspaceName, KeyspaceName, KeyspaceName, KeyspaceName)
	qr1 := utils.Exec(t, conn, query)
	require.EqualValues(t, 4, len(qr1.Fields))

	// test a query with two keyspace names
	query = fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", KeyspaceName, KeyspaceName, "a")
	_, err = conn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "specifying two different database in the query is not supported")
}
