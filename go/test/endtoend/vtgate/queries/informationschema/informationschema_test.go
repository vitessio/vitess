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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t1_id2_idx", "t7_xxhash", "t7_xxhash_idx", "t7_fk"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestDbNameOverride(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("test can randomly select one of the shards, and the shards are in different keyspaces")
	}
	mcmp, closer := start(t)
	defer closer()

	qr, err := mcmp.VtConn.ExecuteFetch("SELECT distinct database() FROM information_schema.tables WHERE table_schema = database()", 1000, true)

	require.Nil(t, err)
	assert.Equal(t, 1, len(qr.Rows), "did not get enough rows back")
	assert.Equal(t, "vt_ks", qr.Rows[0][0].ToString())
}

func TestInformationSchemaQuery(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("test can randomly select one of the shards, and the shards are in different keyspaces")
	}
	mcmp, closer := start(t)
	defer closer()

	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'ks'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'vt_ks'", "vt_ks")
	utils.AssertResultIsEmpty(t, mcmp.VtConn, "table_schema = 'NONE'")
	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'performance_schema'", "performance_schema")
	utils.AssertResultIsEmpty(t, mcmp.VtConn, "table_schema = 'PERFORMANCE_SCHEMA'")
	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'performance_schema' and table_name = 'users'", "performance_schema")
	utils.AssertResultIsEmpty(t, mcmp.VtConn, "table_schema = 'performance_schema' and table_name = 'foo'")
	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'vt_ks' and table_name = 't1'", "vt_ks")
	utils.AssertSingleRowIsReturned(t, mcmp.VtConn, "table_schema = 'ks' and table_name = 't1'", "vt_ks")
}

func TestInformationSchemaWithSubquery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertIsEmpty("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = (SELECT SCHEMA()) AND TABLE_NAME = 'not_exists'")
}

func TestInformationSchemaQueryGetsRoutedToTheRightTableAndKeyspace(t *testing.T) {
	t.Skip("flaky. skipping for now")
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")

	_ = utils.Exec(t, mcmp.VtConn, "SELECT * FROM t1000") // test that the routed table is available to us
	result := utils.Exec(t, mcmp.VtConn, "SELECT * FROM information_schema.tables WHERE table_schema = database() and table_name='t1000'")
	assert.NotEmpty(t, result.Rows)
}

func TestFKConstraintUsingInformationSchema(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	query := "select  fk.referenced_table_name as to_table, fk.referenced_column_name as primary_key, fk.column_name as `column`, fk.constraint_name as name, rc.update_rule as on_update, rc.delete_rule as on_delete from information_schema.referential_constraints as rc join information_schema.key_column_usage as fk on fk.constraint_schema = rc.constraint_schema and fk.constraint_name = rc.constraint_name where fk.referenced_column_name is not null and fk.table_schema = database() and fk.table_name = 't7_fk' and rc.constraint_schema = database() and rc.table_name = 't7_fk'"
	mcmp.AssertMatchesAny(query,
		`[[VARBINARY("t7_xxhash") VARCHAR("uid") VARCHAR("t7_uid") VARCHAR("t7_fk_ibfk_1") BINARY("CASCADE") BINARY("SET NULL")]]`,
		`[[VARCHAR("t7_xxhash") VARCHAR("uid") VARCHAR("t7_uid") VARCHAR("t7_fk_ibfk_1") VARCHAR("CASCADE") VARCHAR("SET NULL")]]`,
		`[[VARCHAR("t7_xxhash") VARCHAR("uid") VARCHAR("t7_uid") VARCHAR("t7_fk_ibfk_1") BINARY("CASCADE") BINARY("SET NULL")]]`)
}

func TestConnectWithSystemSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		vtConnParams := vtParams
		vtConnParams.DbName = dbname
		mysqlConnParams := mysqlParams
		mysqlConnParams.DbName = dbname

		mcmp, err := utils.NewMySQLCompare(t, vtConnParams, mysqlConnParams)
		require.NoError(t, err)
		defer func() {
			mcmp.Close()
		}()

		mcmp.Exec(`select @@max_allowed_packet from dual`)
	}
}

func TestUseSystemSchema(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	for _, dbname := range []string{"information_schema", "mysql", "performance_schema", "sys"} {
		mcmp.Exec(fmt.Sprintf("use %s", dbname))
		mcmp.Exec(`select @@max_allowed_packet from dual`)
	}
}

func TestSystemSchemaQueryWithoutQualifier(t *testing.T) {
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("partial keyspace detected, skipping test")
	}
	mcmp, closer := start(t)
	defer closer()

	queryWithQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", keyspaceName, keyspaceName)
	qr1 := utils.Exec(t, mcmp.VtConn, queryWithQualifier)

	utils.Exec(t, mcmp.VtConn, "use information_schema")
	queryWithoutQualifier := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from tables t "+
		"join columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' "+
		"order by t.table_schema,t.table_name,c.column_name", keyspaceName, keyspaceName)
	qr2 := utils.Exec(t, mcmp.VtConn, queryWithoutQualifier)
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
	if clusterInstance.HasPartialKeyspaces {
		t.Skip("test can randomly select one of the shards, and the shards are in different keyspaces")
	}
	mcmp, closer := start(t)
	defer closer()

	query := fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", keyspaceName, keyspaceName, keyspaceName, keyspaceName)
	qr1 := utils.Exec(t, mcmp.VtConn, query)
	require.EqualValues(t, 4, len(qr1.Fields))

	// test a query with two keyspace names
	query = fmt.Sprintf("select t.table_schema,t.table_name,c.column_name,c.column_type "+
		"from information_schema.tables t "+
		"join information_schema.columns c "+
		"on c.table_schema = t.table_schema and c.table_name = t.table_name "+
		"where t.table_schema = '%s' and c.table_schema = '%s' and c.table_schema = '%s'", keyspaceName, keyspaceName, "a")
	_, err := mcmp.VtConn.ExecuteFetch(query, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "specifying two different database in the query is not supported")
}

func TestInfrSchemaAndUnionAll(t *testing.T) {
	vtConnParams := clusterInstance.GetVTParams(keyspaceName)
	vtConnParams.DbName = keyspaceName
	conn, err := mysql.Connect(context.Background(), &vtConnParams)
	require.NoError(t, err)

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))
			utils.Exec(t, conn, "start transaction")
			utils.Exec(t, conn, `select connection_id()`)
			utils.Exec(t, conn, `(select 'corder' from t1 limit 1) union all (select 'customer' from t7_xxhash limit 1)`)
			utils.Exec(t, conn, "rollback")
		})
	}
}

func TestInfoschemaTypes(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")

	require.NoError(t,
		utils.WaitForAuthoritative(t, "ks", "t1", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`
	SELECT ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = 'ks' AND TABLE_NAME = 't1'
	UNION
	SELECT ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = 'ks' AND TABLE_NAME = 't2';
	`)
}

func TestTypeORMQuery(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	// This test checks that we can run queries similar to the ones that the TypeORM framework uses

	require.NoError(t,
		utils.WaitForAuthoritative(t, "ks", "t1", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp, closer := start(t)
	defer closer()

	utils.AssertMatchesAny(t, mcmp.VtConn, `SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, cols.DATA_TYPE
FROM (SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      WHERE kcu.TABLE_SCHEMA = 'ks'
        AND kcu.TABLE_NAME = 't1'
      UNION
      SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      WHERE kcu.TABLE_SCHEMA = 'ks'
        AND kcu.TABLE_NAME = 't7_xxhash') kcu
         INNER JOIN (SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
                     FROM INFORMATION_SCHEMA.COLUMNS cols
                     WHERE cols.TABLE_SCHEMA = 'ks'
                       AND cols.TABLE_NAME = 't1'
                     UNION
                     SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
                     FROM INFORMATION_SCHEMA.COLUMNS cols
                     WHERE cols.TABLE_SCHEMA = 'ks'
                       AND cols.TABLE_NAME = 't7_xxhash') cols
                    ON kcu.TABLE_SCHEMA = cols.TABLE_SCHEMA AND kcu.TABLE_NAME = cols.TABLE_NAME AND
                       kcu.COLUMN_NAME = cols.COLUMN_NAME`,
		`[[VARBINARY("t1") VARCHAR("id1") BLOB("bigint")] [VARBINARY("t7_xxhash") VARCHAR("uid") BLOB("varchar")]]`,
		`[[VARCHAR("t1") VARCHAR("id1") BLOB("bigint")] [VARCHAR("t7_xxhash") VARCHAR("uid") BLOB("varchar")]]`,
	)

	utils.AssertMatchesAny(t, mcmp.VtConn, `
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ks' AND TABLE_NAME = 't1'
UNION
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'ks' AND TABLE_NAME = 't2';
`,
		`[[VARBINARY("def") VARBINARY("vt_ks") VARBINARY("t1") VARCHAR("id1") UINT32(1) NULL VARCHAR("NO") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("PRI") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARBINARY("def") VARBINARY("vt_ks") VARBINARY("t1") VARCHAR("id2") UINT32(2) NULL VARCHAR("YES") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARBINARY("def") VARBINARY("vt_ks") VARBINARY("t2") VARCHAR("id") UINT32(1) NULL VARCHAR("NO") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("PRI") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARBINARY("def") VARBINARY("vt_ks") VARBINARY("t2") VARCHAR("value") UINT32(2) NULL VARCHAR("YES") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL]]`,
		`[[VARCHAR("def") VARCHAR("vt_ks") VARCHAR("t1") VARCHAR("id1") UINT32(1) NULL VARCHAR("NO") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("PRI") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARCHAR("def") VARCHAR("vt_ks") VARCHAR("t1") VARCHAR("id2") UINT32(2) NULL VARCHAR("YES") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARCHAR("def") VARCHAR("vt_ks") VARCHAR("t2") VARCHAR("id") UINT32(1) NULL VARCHAR("NO") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("PRI") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL] [VARCHAR("def") VARCHAR("vt_ks") VARCHAR("t2") VARCHAR("value") UINT32(2) NULL VARCHAR("YES") BLOB("bigint") NULL NULL UINT64(19) UINT64(0) NULL NULL NULL BLOB("bigint") VARBINARY("") VARCHAR("") VARCHAR("select,insert,update,references") BLOB("") BLOB("") NULL]]`,
	)
}

func TestJoinWithSingleShardQueryOnRHS(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	// This test checks that we can run queries like this, where the RHS is a single shard query
	mcmp, closer := start(t)
	defer closer()

	query := `SELECT
        c.column_name as column_name,
        c.data_type as data_type,
        c.table_name as table_name,
        c.table_schema as table_schema
FROM
        information_schema.columns c
        JOIN (
          SELECT
            table_name
          FROM
            information_schema.tables
          WHERE
            table_schema != 'information_schema'
          LIMIT
            1
        ) AS tables ON tables.table_name = c.table_name
ORDER BY
        c.table_name`

	res := utils.Exec(t, mcmp.VtConn, query)
	require.NotEmpty(t, res.Rows)
}
