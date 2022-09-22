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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestFunctionInDefault(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set the sql mode ALLOW_INVALID_DATES
	utils.Exec(t, conn, `SET sql_mode = 'ALLOW_INVALID_DATES'`)

	utils.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT (TRIM(" check ")))`)
	utils.Exec(t, conn, "drop table function_default")

	utils.Exec(t, conn, `create table function_default (
ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
dt2 DATETIME DEFAULT CURRENT_TIMESTAMP,
ts3 TIMESTAMP DEFAULT 0,
dt3 DATETIME DEFAULT 0,
ts4 TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
dt4 DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
ts5 TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts6 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
dt5 DATETIME ON UPDATE CURRENT_TIMESTAMP,
dt6 DATETIME NOT NULL ON UPDATE CURRENT_TIMESTAMP,
ts7 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
ts8 TIMESTAMP DEFAULT NOW(),
ts9 TIMESTAMP DEFAULT LOCALTIMESTAMP,
ts10 TIMESTAMP DEFAULT LOCALTIME,
ts11 TIMESTAMP DEFAULT LOCALTIMESTAMP(),
ts12 TIMESTAMP DEFAULT LOCALTIME()
)`)
	utils.Exec(t, conn, "drop table function_default")

	// this query works because utc_timestamp will get parenthesised before reaching MySQL. However, this syntax is not supported in MySQL 8.0
	utils.Exec(t, conn, `create table function_default (ts TIMESTAMP DEFAULT UTC_TIMESTAMP)`)
	utils.Exec(t, conn, "drop table function_default")

	utils.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT "check")`)
	utils.Exec(t, conn, "drop table function_default")
}

// TestCheckConstraint test check constraints on CREATE TABLE
// This feature is supported from MySQL 8.0.16 and MariaDB 10.2.1.
func TestCheckConstraint(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := `CREATE TABLE t7 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0), c3 INT CHECK (c3 < 100), CONSTRAINT c1_nonzero CHECK (c1 <> 0), CHECK (c1 > c3));`
	utils.Exec(t, conn, query)

	checkQuery := `SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 't7' order by CONSTRAINT_NAME;`
	expected := `[[VARCHAR("c1_nonzero")] [VARCHAR("c2_positive")] [VARCHAR("t7_chk_1")] [VARCHAR("t7_chk_2")] [VARCHAR("t7_chk_3")] [VARCHAR("t7_chk_4")]]`

	utils.AssertMatches(t, conn, checkQuery, expected)

	cleanup := `DROP TABLE t7`
	utils.Exec(t, conn, cleanup)
}

func TestValueDefault(t *testing.T) {
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `create table test_float_default (pos_f float default 2.1, neg_f float default -2.1,b blob default ('abc'));`)
	defer utils.Exec(t, conn, `drop table test_float_default`)
	utils.AssertMatches(t, conn, "select table_name, column_name, column_default from information_schema.columns where table_name = 'test_float_default' order by column_name", `[[VARBINARY("test_float_default") VARCHAR("b") BLOB("_utf8mb4\\'abc\\'")] [VARBINARY("test_float_default") VARCHAR("neg_f") BLOB("-2.1")] [VARBINARY("test_float_default") VARCHAR("pos_f") BLOB("2.1")]]`)
}

func TestVersionCommentWorks(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "/*!80000 SET SESSION information_schema_stats_expiry=0 */")
	utils.Exec(t, conn, "/*!80000 SET SESSION information_schema_stats_expiry=0 */")
}

func TestSystemVariables(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	tcs := []struct {
		name        string
		value       string
		expectation string
		comment     string
	}{
		{name: "sql_mode", value: "'only_full_group_by'", expectation: `[[VARCHAR("only_full_group_by")]]`},
		{name: "sql_mode", value: "' '", expectation: `[[VARCHAR(" ")]]`},
		{name: "sql_mode", value: "'only_full_group_by'", expectation: `[[VARCHAR("only_full_group_by")]]`, comment: "/* comment */"},
		{name: "sql_mode", value: "' '", expectation: `[[VARCHAR(" ")]]`, comment: "/* comment */"},
	}

	for _, tc := range tcs {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set %s=%s", tc.name, tc.value))
			utils.AssertMatches(t, conn, fmt.Sprintf("select %s @@%s", tc.comment, tc.name), tc.expectation)
		})
	}
}

func TestUseSystemAndUserVariables(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "set @@sql_mode = 'only_full_group_by,strict_trans_tables'")
	utils.Exec(t, conn, "select 1 from information_schema.table_constraints")

	utils.Exec(t, conn, "set @var = @@sql_mode")
	utils.AssertMatches(t, conn, "select @var", `[[VARCHAR("only_full_group_by,strict_trans_tables")]]`)

	utils.Exec(t, conn, "create table t(name varchar(100))")
	utils.Exec(t, conn, "insert into t(name) values (@var)")

	utils.AssertMatches(t, conn, "select name from t", `[[VARCHAR("only_full_group_by,strict_trans_tables")]]`)

	utils.Exec(t, conn, "delete from t where name = @var")
	utils.AssertMatches(t, conn, "select name from t", `[]`)

	utils.Exec(t, conn, "drop table t")
}

func BenchmarkReservedConnWhenSettingSysVar(b *testing.B) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(b, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("create table t(id int)", 1000, true)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		_, err = conn.ExecuteFetch("drop table t", 1000, true)
		if err != nil {
			b.Fatal(err)
		}
	}()

	_, err = conn.ExecuteFetch("set @@sql_mode = 'only_full_group_by,strict_trans_tables', @@sql_big_selects = 0, @@sql_safe_updates = 1, @@foreign_key_checks = 0", 1000, true)
	if err != nil {
		b.Fatal(err)
	}

	f := func(i int) {
		_, err = conn.ExecuteFetch(fmt.Sprintf("insert into t(id) values (%d)", i), 1, true)
		if err != nil {
			b.Fatal(err)
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("select id from t where id = %d limit 1", i), 1, true)
		if err != nil {
			b.Fatal(err)
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("update t set id = 1 where id = %d", i), 1, true)
		if err != nil {
			b.Fatal(err)
		}
		_, err = conn.ExecuteFetch(fmt.Sprintf("delete from t where id = %d", i), 1, true)
		if err != nil {
			b.Fatal(err)
		}
	}

	// warmup, plan and cache the plans
	f(0)

	benchmarkName := "Use SET_VAR"
	for i := 0; i < 2; i++ {
		b.Run(benchmarkName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f(i)
			}
		})

		// setting another sysvar that does not support SET_VAR, the next iteration of benchmark will use reserved connection
		_, err = conn.ExecuteFetch("set @@sql_warnings = 1", 1, true)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkName = "Use reserved connections"
	}
}

func TestJsonFunctions(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn,
		`SELECT 
JSON_QUOTE('null'), 
JSON_QUOTE('"null"'), 
JSON_OBJECT(BIN(1),2,'abc',ASCII(4)), 
JSON_ARRAY(1, "abc", NULL, TRUE)`,
		`[[VARBINARY("\"null\"") VARBINARY("\"\\\"null\\\"\"") JSON("{\"1\": 2, \"abc\": 52}") JSON("[1, \"abc\", null, true]")]]`)

	utils.AssertMatches(t, conn,
		`SELECT 
JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1'), 
JSON_CONTAINS_PATH('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.e'), 
JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]'), 
JSON_UNQUOTE(JSON_EXTRACT('["a","b"]', '$[1]')), 
JSON_KEYS('{"a": 1, "b": {"c": 30}}'), 
JSON_OVERLAPS("[1,3,5,7]", "[2,5,7]"), 
JSON_SEARCH('["abc"]', 'one', 'abc'), 
JSON_VALUE('{"fname": "Joe", "lname": "Palmer"}', '$.fname'), 
JSON_ARRAY(4,5) MEMBER OF('[[3,4],[4,5]]')`,
		`[[INT64(0) INT64(1) JSON("20") BLOB("b") JSON("[\"a\", \"b\"]") INT64(1) JSON("\"$[0]\"") VARBINARY("Joe") INT64(1)]]`)

	utils.AssertMatches(t, conn,
		`SELECT 
JSON_SCHEMA_VALIDATION_REPORT('{"type":"string","pattern":"("}', '"abc"'), 
JSON_SCHEMA_VALID('{"type":"string","pattern":"("}', '"abc"');`,
		`[[JSON("{\"valid\": true}") INT64(1)]]`)

	utils.Exec(t, conn, "create table jt(a JSON, b INT)")
	utils.Exec(t, conn, `INSERT INTO jt (a, b) VALUES ("[3,10,5,\"x\",44]", 33), ("[3,10,5,17,[22,44,66]]", 0)`)
	defer func() {
		utils.Exec(t, conn, "drop table jt")
	}()

	utils.AssertMatches(t, conn,
		`SELECT a->"$[4]", a->>"$[3]" FROM jt`,
		`[[JSON("44") BLOB("x")] [JSON("[22, 44, 66]") BLOB("17")]]`)

	utils.AssertMatches(t, conn,
		`select JSON_DEPTH('{}'), JSON_LENGTH('{"a": 1, "b": {"c": 30}}', '$.b'), JSON_TYPE(JSON_EXTRACT('{"a": [10, true]}', '$.a')), JSON_VALID('{"a": 1}');`,
		`[[INT64(1) INT64(1) VARBINARY("ARRAY") INT64(1)]]`)

	utils.AssertMatches(t, conn,
		`select 
JSON_ARRAY_APPEND('{"a": 1}', '$', 'z'), 
JSON_ARRAY_INSERT('["a", {"b": [1, 2]}, [3, 4]]', '$[0]', 'x', '$[2][1]', 'y'), 
JSON_INSERT('{ "a": 1, "b": [2, 3]}', '$.a', 10, '$.c', CAST('[true, false]' AS JSON))`,
		`[[JSON("[{\"a\": 1}, \"z\"]") JSON("[\"x\", \"a\", {\"b\": [1, 2]}, [3, 4]]") JSON("{\"a\": 1, \"b\": [2, 3], \"c\": [true, false]}")]]`)

	utils.AssertMatches(t, conn,
		`select 
JSON_MERGE('[1, 2]', '[true, false]'), 
JSON_MERGE_PATCH('{"name": "x"}', '{"id": 47}'),
JSON_MERGE_PRESERVE('[1, 2]', '{"id": 47}')`,
		`[[JSON("[1, 2, true, false]") JSON("{\"id\": 47, \"name\": \"x\"}") JSON("[1, 2, {\"id\": 47}]")]]`)

	utils.AssertMatches(t, conn,
		`select 
JSON_REMOVE('[1, [2, 3], 4]', '$[1]'), 
JSON_REPLACE('{ "a": 1, "b": [2, 3]}', '$.a', 10, '$.c', '[true, false]'), 
JSON_SET('{ "a": 1, "b": [2, 3]}', '$.a', 10, '$.c', '[true, false]'), 
JSON_UNQUOTE('"abc"')`,
		`[[JSON("[1, 4]") JSON("{\"a\": 10, \"b\": [2, 3]}") JSON("{\"a\": 10, \"b\": [2, 3], \"c\": \"[true, false]\"}") VARBINARY("abc")]]`)
}
