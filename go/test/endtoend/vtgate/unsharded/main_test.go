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
	"flag"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	KeyspaceName    = "customer"
	SchemaSQL       = `
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
) ENGINE=Innodb;`
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

	createProcSQL = `use vt_customer;
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
    set autocommit = 0;
END;

CREATE PROCEDURE in_parameter(IN val int)
BEGIN
	insert into allDefaults(id) values(val);
END;

CREATE PROCEDURE out_parameter(OUT val int)
BEGIN
	insert into allDefaults(id) values (128);
	select 128 into val from dual;
END;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		Keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		clusterInstance.VtTabletExtraArgs = []string{"--queryserver-config-transaction-timeout", "3", "--queryserver-config-max-result-size", "30"}
		if err := clusterInstance.StartUnshardedKeyspace(*Keyspace, 0, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"--warn_sharded_only=true"}
		if err := clusterInstance.StartVtgate(); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess
		if _, err := primaryTablet.QueryTablet(createProcSQL, KeyspaceName, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestSelectIntoAndLoadFrom(t *testing.T) {
	// Test is skipped because it requires secure-file-priv variable to be set to not NULL or empty.
	t.Skip()
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc')`)
	res := utils.Exec(t, conn, `select @@secure_file_priv;`)
	directory := res.Rows[0][0].ToString()
	query := `select * from t1 into outfile '` + directory + `x.txt'`
	utils.Exec(t, conn, query)
	defer os.Remove(directory + `x.txt`)
	query = `load data infile '` + directory + `x.txt' into table t1`
	utils.AssertContainsError(t, conn, query, "Duplicate entry '300' for key 'PRIMARY'")
	utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, query)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
	query = `select * from t1 into dumpfile '` + directory + `x1.txt'`
	utils.Exec(t, conn, query)
	defer os.Remove(directory + `x1.txt`)
	query = `select * from t1 into outfile '` + directory + `x2.txt' Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	utils.Exec(t, conn, query)
	defer os.Remove(directory + `x2.txt`)
	query = `load data infile '` + directory + `x2.txt' replace into table t1 Fields terminated by ';' optionally enclosed by '"' escaped by '\t' lines terminated by '\n'`
	utils.Exec(t, conn, query)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)]]`)
	utils.AssertMatches(t, conn, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("use of feature that is only supported in unsharded mode: LOAD")]]`)
}

func TestEmptyStatement(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, `delete from t1`)
	utils.AssertContainsError(t, conn, " \t; \n;", "Query was empty")
	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc');         ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)

	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

func TestTopoDownServingQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)

	execMulti(t, conn, `insert into t1(c1, c2, c3, c4) values (300,100,300,'abc'); ;; insert into t1(c1, c2, c3, c4) values (301,101,301,'abcd');;`)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
	clusterInstance.TopoProcess.TearDown(clusterInstance.Cell, clusterInstance.OriginalVTDATAROOT, clusterInstance.CurrentVTDATAROOT, true, *clusterInstance.TopoFlavorString())
	time.Sleep(3 * time.Second)
	utils.AssertMatches(t, conn, `select c1,c2,c3 from t1`, `[[INT64(300) INT64(100) INT64(300)] [INT64(301) INT64(101) INT64(301)]]`)
}

func TestInsertAllDefaults(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `insert into allDefaults () values ()`)
	utils.AssertMatches(t, conn, `select * from allDefaults`, "[[INT64(1) NULL]]")
}

func TestDDLUnsharded(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `create table tempt1(c1 BIGINT NOT NULL,c2 BIGINT NOT NULL,c3 BIGINT,c4 varchar(100),PRIMARY KEY (c1), UNIQUE KEY (c2),UNIQUE KEY (c3), UNIQUE KEY (c4))`)
	// Test that create view works and the output is as expected
	utils.Exec(t, conn, `create view v1 as select * from tempt1`)
	utils.Exec(t, conn, `insert into tempt1(c1, c2, c3, c4) values (300,100,300,'abc'),(30,10,30,'ac'),(3,0,3,'a')`)
	utils.AssertMatches(t, conn, "select * from v1", `[[INT64(3) INT64(0) INT64(3) VARCHAR("a")] [INT64(30) INT64(10) INT64(30) VARCHAR("ac")] [INT64(300) INT64(100) INT64(300) VARCHAR("abc")]]`)
	utils.Exec(t, conn, `drop view v1`)
	utils.Exec(t, conn, `drop table tempt1`)
	utils.AssertMatchesAny(t, conn, "show tables", `[[VARBINARY("allDefaults")] [VARBINARY("t1")]]`, `[[VARCHAR("allDefaults")] [VARCHAR("t1")]]`)
}

func TestCallProcedure(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		Flags:  mysql.CapabilityClientMultiResults,
		DbName: "@primary",
	}
	time.Sleep(5 * time.Second)
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	qr := utils.Exec(t, conn, `CALL sp_insert()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	utils.AssertMatches(t, conn, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("'CALL' not supported in sharded mode")]]`)

	_, err = conn.ExecuteFetch(`CALL sp_select()`, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multi-Resultset not supported in stored procedure")

	_, err = conn.ExecuteFetch(`CALL sp_all()`, 1000, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Multi-Resultset not supported in stored procedure")

	qr = utils.Exec(t, conn, `CALL sp_delete()`)
	require.GreaterOrEqual(t, 1, int(qr.RowsAffected))

	qr = utils.Exec(t, conn, `CALL sp_multi_dml()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	qr = utils.Exec(t, conn, `CALL sp_variable()`)
	require.EqualValues(t, 1, qr.RowsAffected)

	qr = utils.Exec(t, conn, `CALL in_parameter(42)`)
	require.EqualValues(t, 1, qr.RowsAffected)

	_ = utils.Exec(t, conn, `SET @foo = 123`)
	qr = utils.Exec(t, conn, `CALL in_parameter(@foo)`)
	require.EqualValues(t, 1, qr.RowsAffected)
	qr = utils.Exec(t, conn, "select * from allDefaults where id = 123")
	assert.NotEmpty(t, qr.Rows)

	_, err = conn.ExecuteFetch(`CALL out_parameter(@foo)`, 100, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "OUT and INOUT parameters are not supported")
}

func TestTempTable(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn1.Close()

	_ = utils.Exec(t, conn1, `create temporary table temp_t(id bigint primary key)`)
	utils.AssertMatches(t, conn1, "show warnings", `[[VARCHAR("Warning") UINT16(1235) VARCHAR("'temporary table' not supported in sharded mode")]]`)
	_ = utils.Exec(t, conn1, `insert into temp_t(id) values (1),(2),(3)`)
	utils.AssertMatches(t, conn1, `select id from temp_t order by id`, `[[INT64(1)] [INT64(2)] [INT64(3)]]`)
	utils.AssertMatches(t, conn1, `select count(table_id) from information_schema.innodb_temp_table_info`, `[[INT64(1)]]`)

	conn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	utils.AssertMatches(t, conn2, `select count(table_id) from information_schema.innodb_temp_table_info`, `[[INT64(1)]]`)
	utils.AssertContainsError(t, conn2, `show create table temp_t`, `Table 'vt_customer.temp_t' doesn't exist (errno 1146) (sqlstate 42S02)`)
}

func TestReservedConnDML(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `set default_week_format = 1`)
	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `insert into allDefaults () values ()`)
	utils.Exec(t, conn, `commit`)

	time.Sleep(6 * time.Second)

	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `insert into allDefaults () values ()`)
	utils.Exec(t, conn, `commit`)
}

func TestNumericPrecisionScale(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, "CREATE TABLE `a` (`one` bigint NOT NULL PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	require.NoError(t, err)
	defer utils.Exec(t, conn, "drop table `a`")

	qr := utils.Exec(t, conn, "select numeric_precision, numeric_scale from information_schema.columns where table_name = 'a'")
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
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete t1 from t1 where c1 = 1")
	utils.Exec(t, conn, "delete t.* from t1 t where t.c1 = 1")
}

func TestFloatValueDefault(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `create table test_float_default (pos_f float default 2.1, neg_f float default -2.1);`)
	defer utils.Exec(t, conn, `drop table test_float_default`)
	utils.AssertMatchesAny(t, conn, "select table_name, column_name, column_default from information_schema.columns where table_name = 'test_float_default' order by column_default desc",
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
