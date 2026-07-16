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

package unsharded

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vitesst"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	KeyspaceName = "customer"
	SchemaSQL    = `
CREATE TABLE t1 (
    c1 BIGINT NOT NULL,
    c2 BIGINT NOT NULL,
    c3 BIGINT,
    c4 varchar(100),
    PRIMARY KEY (c1),
    UNIQUE KEY (c2),
    UNIQUE KEY (c3),
    UNIQUE KEY (c4)
) ENGINE=Innodb;

CREATE TABLE allDefaults (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255)
) ENGINE=Innodb;

CREATE PROCEDURE sp_insert()
BEGIN
	insert into allDefaults () values ();
END;

CREATE PROCEDURE sp_delete()
BEGIN
	delete from allDefaults;
END;

CREATE PROCEDURE sp_multi_dml()
BEGIN
	insert into allDefaults () values ();
	delete from allDefaults;
END;

CREATE PROCEDURE sp_variable()
BEGIN
	insert into allDefaults () values ();
	SELECT min(id) INTO @myvar FROM allDefaults;
	DELETE FROM allDefaults WHERE id = @myvar;
END;

CREATE PROCEDURE sp_select()
BEGIN
	SELECT * FROM allDefaults;
END;

CREATE PROCEDURE sp_all()
BEGIN
	insert into allDefaults () values ();
    select * from allDefaults;
	delete from allDefaults;
END;`

	VSchema = `
{
    "sharded": false,
    "tables": {
        "t1": {
            "columns": [
                {
                    "name": "c1",
                    "type": "INT64"
                },
                {
                    "name": "c2",
                    "type": "INT64"
                },
                {
                    "name": "c3",
                    "type": "INT64"
                },
                {
                    "name": "c4",
                    "type": "VARCHAR"
                }
            ]
        },
        "allDefaults": {
            "columns": [
                {
                    "name": "id",
                    "type": "INT64"
                },
                {
                    "name": "name",
                    "type": "VARCHAR"
                }
            ]
        }
    }
}
`

	createProcSQL = []string{
		`
CREATE PROCEDURE in_parameter(IN val int)
BEGIN
	insert into allDefaults(id) values(val);
END;
`, `
CREATE PROCEDURE out_parameter(OUT val int)
BEGIN
	insert into allDefaults(id) values (128);
	select 128 into val from dual;
END;
`,
		`CREATE DEFINER=current_user() PROCEDURE with_definer(OUT val int)
BEGIN
	insert into allDefaults(id) values (128);
	select 128 into val from dual;
END;
`,
		`CREATE PROCEDURE p1 (in x BIGINT) BEGIN declare y DECIMAL(14,2); set y = 4.2; END`,
		`CREATE PROCEDURE p2 (in x BIGINT) BEGIN START TRANSACTION; SELECT 128 from dual; COMMIT; END`,
	}
)

func setup(t *testing.T) (*vitesst.Cluster, mysql.ConnParams) {
	t.Helper()

	ctx := t.Context()
	cluster, err := vitesst.NewCluster(
		vitesst.WithVTTabletArgs("--queryserver-config-transaction-timeout", "3s", "--queryserver-config-max-result-size", "30"),
		vitesst.WithVTGateArgs("--warn-sharded-only=true"),
		vitesst.WithKeyspace(KeyspaceName).
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
	)
	require.NoError(t, err)
	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		require.NoError(t, cleanup(cleanupCtx))
	})

	vtParams := cluster.VTParams(ctx, "")
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	require.NoError(t, runCreateProcedures(conn))

	return cluster, vtParams
}

func runCreateProcedures(conn *mysql.Conn) error {
	for _, sql := range createProcSQL {
		_, err := conn.ExecuteFetch(sql, 1000, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSelectIntoAndLoadFrom(t *testing.T) {
	// Test is skipped because it requires secure-file-priv variable to be set to not NULL or empty.
	t.Skip()
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer vitesst.Exec(t, conn, `delete from t1`)
	vitesst.Exec(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc')`)
	res := vitesst.Exec(t, conn, `select @@secure_file_priv;`)
	directory := res.Rows[0][0].ToString()
	query := `select * from t1 into outfile '` + directory + `x.txt'`
	vitesst.Exec(t, conn, query)
	defer os.Remove(directory + `x.txt`)
	query = `load data infile '` + directory + `x.txt' into table t1`
	vitesst.AssertContainsError(t, conn, query, "Duplicate entry '300' for key 'PRIMARY'")
	vitesst.Exec(t, conn, `delete from t1`)
	vitesst.Exec(t, conn, query)
	vitesst.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
	query = `select * from t1 into dumpfile '` + directory + `x1.txt'`
	vitesst.Exec(t, conn, query)
	defer os.Remove(directory + `x1.txt`)
	query = `select * from t1 into outfile '` + directory + `x2.txt' Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	vitesst.Exec(t, conn, query)
	defer os.Remove(directory + `x2.txt`)
	query = `load data infile '` + directory + `x2.txt' replace into table t1 Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	vitesst.Exec(t, conn, query)
	vitesst.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
	vitesst.AssertMatches(t, conn, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("use of feature that is only supported in unsharded mode: LOAD")]]`)
}

func TestEmptyStatement(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	defer vitesst.Exec(t, conn, `delete from t1`)
	vitesst.AssertContainsError(t, conn, " \t; \n;", "Query was empty")
	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc');         ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)

	vitesst.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

func TestTopoDownServingQuery(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer vitesst.Exec(t, conn, `delete from t1`)

	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
	vitesst.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
	require.NoError(t, clusterInstance.Topo().StopContainer(ctx, 30*time.Second))
	t.Cleanup(func() {
		require.NoError(t, clusterInstance.Topo().StartContainer(context.WithoutCancel(ctx)))
	})
	time.Sleep(3 * time.Second)
	vitesst.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

func TestInsertAllDefaults(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, `insert into allDefaults () values ()`)
	vitesst.AssertMatches(t, conn, `select * from allDefaults`, "[[INT64(1) NULL]]")
}

func TestDDLUnsharded(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, `create table tempt1(c1 BIGINT NOT NULL,c2 BIGINT NOT NULL,c3 BIGINT,c4 varchar(100),PRIMARY KEY (c1), UNIQUE KEY (c2),UNIQUE KEY (c3), UNIQUE KEY (c4))`)
	// Test that create view works and the output is as expected
	vitesst.Exec(t, conn, `create view v1 as select * from tempt1`)
	vitesst.Exec(t, conn, `insert into tempt1(c1, c2, c3, c4) values (300,100,300,'abc'),(30,10,30,'ac'),(3,0,3,'a')`)
	vitesst.AssertMatches(t, conn, "select * from v1", `[[INT64(3) INT64(0) INT64(3) VARCHAR("a")] [INT64(30) INT64(10) INT64(30) VARCHAR("ac")] [INT64(300) INT64(100) INT64(300) VARCHAR("abc")]]`)
	vitesst.Exec(t, conn, `drop view v1`)
	vitesst.Exec(t, conn, `drop table tempt1`)
	vitesst.AssertMatchesAny(t, conn, "show tables", `[[VARBINARY("allDefaults")] [VARBINARY("t1")]]`, `[[VARCHAR("allDefaults")] [VARCHAR("t1")]]`)
}

func TestCallProcedure(t *testing.T) {
	ctx := t.Context()
	clusterInstance, _ := setup(t)
	vtParams := clusterInstance.VTParams(ctx, "@primary")
	vtParams.Flags = mysql.CapabilityClientMultiResults
	time.Sleep(5 * time.Second)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	qr := vitesst.Exec(t, conn, `CALL sp_insert()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	vitesst.AssertMatches(t, conn, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("'CALL' not supported in sharded mode")]]`)

	err = conn.ExecuteFetchMultiDrain(`CALL sp_select()`)
	require.ErrorContains(t, err, "Multi-Resultset not supported in stored procedure")

	err = conn.ExecuteFetchMultiDrain(`CALL sp_all()`)
	require.ErrorContains(t, err, "Multi-Resultset not supported in stored procedure")

	qr = vitesst.Exec(t, conn, `CALL sp_delete()`)
	require.GreaterOrEqual(t, 1, int(qr.RowsAffected))

	qr = vitesst.Exec(t, conn, `CALL sp_multi_dml()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	qr = vitesst.Exec(t, conn, `CALL sp_variable()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	qr = vitesst.Exec(t, conn, `CALL in_parameter(42)`)
	require.EqualValues(t, 1, qr.RowsAffected)

	_ = vitesst.Exec(t, conn, `SET @foo = 123`)
	qr = vitesst.Exec(t, conn, `CALL in_parameter(@foo)`)
	require.EqualValues(t, 1, qr.RowsAffected)
	qr = vitesst.Exec(t, conn, "select * from allDefaults where id = 123")
	assert.NotEmpty(t, qr.Rows)

	_, err = conn.ExecuteFetch(`CALL out_parameter(@foo)`, 100, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "OUT and INOUT parameters are not supported")
}

func TestTempTable(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	_ = vitesst.Exec(t, conn1, `create temporary table temp_t(id bigint primary key)`)
	vitesst.AssertMatches(t, conn1, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("'temporary table' not supported in sharded mode")]]`)
	_ = vitesst.Exec(t, conn1, `insert into temp_t(id) values (1),(2),(3)`)
	vitesst.AssertMatches(t, conn1, `select id from temp_t order by id`, `[[INT64(1)] [INT64(2)] [INT64(3)]]`)
	vitesst.AssertMatches(t, conn1, `select count(table_id) from information_schema.innodb_temp_table_info`, `[[INT64(1)]]`)

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	vitesst.AssertMatches(t, conn2, `select count(table_id) from information_schema.innodb_temp_table_info`, `[[INT64(1)]]`)
	vitesst.AssertContainsError(t, conn2, `show create table temp_t`, `Table 'vt_customer.temp_t' doesn't exist (errno 1146) (sqlstate 42S02)`)
}

func TestReservedConnDML(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, `set default_week_format = 1`)
	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `insert into allDefaults () values ()`)
	vitesst.Exec(t, conn, `commit`)

	time.Sleep(6 * time.Second)

	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `insert into allDefaults () values ()`)
	vitesst.Exec(t, conn, `commit`)
}

func TestNumericPrecisionScale(t *testing.T) {
	ctx := t.Context()
	_, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = vitesst.Exec(t, conn, "CREATE TABLE `a` (`one` bigint NOT NULL PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	require.NoError(t, err)
	defer vitesst.Exec(t, conn, "drop table `a`")

	qr := vitesst.Exec(t, conn, "select numeric_precision, numeric_scale from information_schema.columns where table_name = 'a'")
	require.Equal(t, 1, len(qr.Rows))

	/*
		We expect UINT64 to be returned as type for field and rows from VTGate to client.

		require.Equal(t, querypb.Type_UINT64, qr.Fields[0].Type)
		require.Equal(t, querypb.Type_UINT64, qr.Fields[1].Type)
		require.Equal(t, sqltypes.Uint64, qr.Rows[0][0].Type())
		require.Equal(t, sqltypes.Uint64, qr.Rows[0][1].Type())

		But, the field query from mysql returns field at UINT32 and row types as UINT64.
		Our conversion on VTGate on receiving data from VTTablet the Rows are converted to Field Types.
		So, we see UINT32 for both fields and rows.

		This issue is only with MySQL 8.0. In CI we use 5.7 as well. So asserting with both the values.
	*/

	assert.True(t, qr.Fields[0].Type == querypb.Type_UINT64 || qr.Fields[0].Type == querypb.Type_UINT32)
	assert.True(t, qr.Fields[1].Type == querypb.Type_UINT64 || qr.Fields[1].Type == querypb.Type_UINT32)
	assert.True(t, qr.Rows[0][0].Type() == sqltypes.Uint64 || qr.Rows[0][0].Type() == sqltypes.Uint32)
	assert.True(t, qr.Rows[0][1].Type() == sqltypes.Uint64 || qr.Rows[0][1].Type() == sqltypes.Uint32)
}

func TestDeleteAlias(t *testing.T) {
	_, vtParams := setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "delete t1 from t1 where c1 = 1")
	vitesst.Exec(t, conn, "delete t.* from t1 t where t.c1 = 1")
}

func TestFloatValueDefault(t *testing.T) {
	_, vtParams := setup(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, `create table test_float_default (pos_f float default 2.1, neg_f float default -2.1);`)
	defer vitesst.Exec(t, conn, `drop table test_float_default`)
	vitesst.AssertMatchesAny(t, conn, "select table_name, column_name, column_default from information_schema.columns where table_name = 'test_float_default' order by column_default desc",
		`[[VARBINARY("test_float_default") VARCHAR("pos_f") BLOB("2.1")] [VARBINARY("test_float_default") VARCHAR("neg_f") BLOB("-2.1")]]`,
		`[[VARCHAR("test_float_default") VARCHAR("pos_f") TEXT("2.1")] [VARCHAR("test_float_default") VARCHAR("neg_f") TEXT("-2.1")]]`)
}

func execMulti(t *testing.T, conn *mysql.Conn, query string) []*sqltypes.Result {
	t.Helper()
	var res []*sqltypes.Result
	qr, more, err := conn.ExecuteFetchMulti(query, 1000, true)
	res = append(res, qr)
	require.NoError(t, err)
	for more == true {
		qr, more, _, err = conn.ReadQueryResult(1000, true)
		require.NoError(t, err)
		res = append(res, qr)
	}
	return res
}

// TestMetricForExplain verifies that query metrics are correctly published for explain queries.
func TestMetricForExplain(t *testing.T) {
	ctx := t.Context()
	clusterInstance, vtParams := setup(t)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	initialQP := getQPMetric(t, clusterInstance, "QueryExecutions")
	initialQT := getQPMetric(t, clusterInstance, "QueryExecutionsByTable")

	t.Run("explain t1", func(t *testing.T) {
		vitesst.Exec(t, conn, "explain t1")
		updatedQP := getQPMetric(t, clusterInstance, "QueryExecutions")
		updatedQT := getQPMetric(t, clusterInstance, "QueryExecutionsByTable")
		assert.EqualValues(t, 1, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"))
		assert.EqualValues(t, 1, getValue(updatedQT, "EXPLAIN.customer_t1")-getValue(initialQT, "EXPLAIN.customer_t1"))
	})

	t.Run("explain `select c1, c2 from t1`", func(t *testing.T) {
		vitesst.ExecAllowError(t, conn, "explain `select c1, c2 from t1`")
		updatedQP := getQPMetric(t, clusterInstance, "QueryExecutions")
		updatedQT := getQPMetric(t, clusterInstance, "QueryExecutionsByTable")
		assert.EqualValues(t, 2, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"))
		assert.EqualValues(t, 1, getValue(updatedQT, "EXPLAIN.customer_t1")-getValue(initialQT, "EXPLAIN.customer_t1"))
	})

	t.Run("explain select c1, c2 from t1", func(t *testing.T) {
		vitesst.Exec(t, conn, "explain select c1, c2 from t1")
		updatedQP := getQPMetric(t, clusterInstance, "QueryExecutions")
		updatedQT := getQPMetric(t, clusterInstance, "QueryExecutionsByTable")
		assert.EqualValues(t, 3, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"))
		assert.EqualValues(t, 2, getValue(updatedQT, "EXPLAIN.customer_t1")-getValue(initialQT, "EXPLAIN.customer_t1"))
	})
}

func getQPMetric(t *testing.T, cluster *vitesst.Cluster, metric string) map[string]any {
	t.Helper()

	vars, err := cluster.VTGate().GetVars(t.Context())
	require.NoError(t, err)
	require.NotNil(t, vars)

	qpVars, exists := vars[metric]
	if !exists {
		return nil
	}

	qpMap, ok := qpVars.(map[string]any)
	require.True(t, ok, "query queryMetric vars is not a map")

	return qpMap
}

func getValue(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	val, exists := m[key]
	if !exists {
		return 0
	}
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}
