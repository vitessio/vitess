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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sysvars"

	"github.com/stretchr/testify/require"
)

type myTestCase struct {
	in, expected                                                       string
	liid, db, foundRows, rowCount, rawGTID, rawTimeout, sessTrackGTID  bool
	ddlStrategy, sessionUUID, sessionEnableSystemSettings              bool
	udv                                                                int
	autocommit, clientFoundRows, skipQueryPlanCache, socket            bool
	sqlSelectLimit, transactionMode, workload, version, versionComment bool
}

func TestRewrites(in *testing.T) {
	tests := []myTestCase{{
		in:       "SELECT 42",
		expected: "SELECT 42",
		// no bindvar needs
	}, {
		in:       "SELECT @@version",
		expected: "SELECT :__vtversion as `@@version`",
		version:  true,
	}, {
		in:             "SELECT @@version_comment",
		expected:       "SELECT :__vtversion_comment as `@@version_comment`",
		versionComment: true,
	}, {
		in:                          "SELECT @@enable_system_settings",
		expected:                    "SELECT :__vtenable_system_settings as `@@enable_system_settings`",
		sessionEnableSystemSettings: true,
	}, {
		in:       "SELECT last_insert_id()",
		expected: "SELECT :__lastInsertId as `last_insert_id()`",
		liid:     true,
	}, {
		in:       "SELECT database()",
		expected: "SELECT :__vtdbname as `database()`",
		db:       true,
	}, {
		in:       "SELECT database() from test",
		expected: "SELECT database() from test",
		// no bindvar needs
	}, {
		in:       "SELECT last_insert_id() as test",
		expected: "SELECT :__lastInsertId as test",
		liid:     true,
	}, {
		in:       "SELECT last_insert_id() + database()",
		expected: "SELECT :__lastInsertId + :__vtdbname as `last_insert_id() + database()`",
		db:       true, liid: true,
	}, {
		// unnest database() call
		in:       "select (select database()) from test",
		expected: "select database() as `(select database() from dual)` from test",
		// no bindvar needs
	}, {
		// unnest database() call
		in:       "select (select database() from dual) from test",
		expected: "select database() as `(select database() from dual)` from test",
		// no bindvar needs
	}, {
		in:       "select (select database() from dual) from dual",
		expected: "select :__vtdbname as `(select database() from dual)` from dual",
		db:       true,
	}, {
		in:       "select id from user where database()",
		expected: "select id from user where database()",
		// no bindvar needs
	}, {
		in:       "select table_name from information_schema.tables where table_schema = database()",
		expected: "select table_name from information_schema.tables where table_schema = database()",
		// no bindvar needs
	}, {
		in:       "select schema()",
		expected: "select :__vtdbname as `schema()`",
		db:       true,
	}, {
		in:        "select found_rows()",
		expected:  "select :__vtfrows as `found_rows()`",
		foundRows: true,
	}, {
		in:       "select @`x y`",
		expected: "select :__vtudvx_y as `@``x y``` from dual",
		udv:      1,
	}, {
		in:       "select id from t where id = @x and val = @y",
		expected: "select id from t where id = :__vtudvx and val = :__vtudvy",
		db:       false, udv: 2,
	}, {
		in:       "insert into t(id) values(@xyx)",
		expected: "insert into t(id) values(:__vtudvxyx)",
		db:       false, udv: 1,
	}, {
		in:       "select row_count()",
		expected: "select :__vtrcount as `row_count()`",
		rowCount: true,
	}, {
		in:       "SELECT lower(database())",
		expected: "SELECT lower(:__vtdbname) as `lower(database())`",
		db:       true,
	}, {
		in:         "SELECT @@autocommit",
		expected:   "SELECT :__vtautocommit as `@@autocommit`",
		autocommit: true,
	}, {
		in:              "SELECT @@client_found_rows",
		expected:        "SELECT :__vtclient_found_rows as `@@client_found_rows`",
		clientFoundRows: true,
	}, {
		in:                 "SELECT @@skip_query_plan_cache",
		expected:           "SELECT :__vtskip_query_plan_cache as `@@skip_query_plan_cache`",
		skipQueryPlanCache: true,
	}, {
		in:             "SELECT @@sql_select_limit",
		expected:       "SELECT :__vtsql_select_limit as `@@sql_select_limit`",
		sqlSelectLimit: true,
	}, {
		in:              "SELECT @@transaction_mode",
		expected:        "SELECT :__vttransaction_mode as `@@transaction_mode`",
		transactionMode: true,
	}, {
		in:       "SELECT @@workload",
		expected: "SELECT :__vtworkload as `@@workload`",
		workload: true,
	}, {
		in:       "SELECT @@socket",
		expected: "SELECT :__vtsocket as `@@socket`",
		socket:   true,
	}, {
		in:       "select (select 42) from dual",
		expected: "select 42 as `(select 42 from dual)` from dual",
	}, {
		in:       "select * from user where col = (select 42)",
		expected: "select * from user where col = 42",
	}, {
		in:       "select * from (select 42) as t", // this is not an expression, and should not be rewritten
		expected: "select * from (select 42) as t",
	}, {
		in:       `select (select (select (select (select (select last_insert_id()))))) as x`,
		expected: "select :__lastInsertId as x from dual",
		liid:     true,
	}, {
		in:          `select * from user where col = @@ddl_strategy`,
		expected:    "select * from user where col = :__vtddl_strategy",
		ddlStrategy: true,
	}, {
		in:       `select * from user where col = @@read_after_write_gtid OR col = @@read_after_write_timeout OR col = @@session_track_gtids`,
		expected: "select * from user where col = :__vtread_after_write_gtid or col = :__vtread_after_write_timeout or col = :__vtsession_track_gtids",
		rawGTID:  true, rawTimeout: true, sessTrackGTID: true,
	}, {
		in:       "SELECT a.col, b.col FROM A JOIN B USING (id)",
		expected: "SELECT a.col, b.col FROM A JOIN B ON A.id = B.id",
	}, {
		in:       "SELECT a.col, b.col FROM A JOIN B USING (id1,id2,id3)",
		expected: "SELECT a.col, b.col FROM A JOIN B ON A.id1 = B.id1 AND A.id2 = B.id2 AND A.id3 = B.id3",
	}, {
		// SELECT * behaves different depending the join type used, so if that has been used, we won't rewrite
		in:       "SELECT * FROM A JOIN B USING (id1,id2,id3)",
		expected: "SELECT * FROM A JOIN B USING (id1,id2,id3)",
	}, {
		in:       "CALL proc(@foo)",
		expected: "CALL proc(:__vtudvfoo)",
		udv:      1,
	}, {
		in:                          "SHOW VARIABLES",
		expected:                    "SHOW VARIABLES",
		autocommit:                  true,
		clientFoundRows:             true,
		skipQueryPlanCache:          true,
		sqlSelectLimit:              true,
		transactionMode:             true,
		workload:                    true,
		version:                     true,
		versionComment:              true,
		ddlStrategy:                 true,
		sessionUUID:                 true,
		sessionEnableSystemSettings: true,
		rawGTID:                     true,
		rawTimeout:                  true,
		sessTrackGTID:               true,
		socket:                      true,
	}, {
		in:                          "SHOW GLOBAL VARIABLES",
		expected:                    "SHOW GLOBAL VARIABLES",
		autocommit:                  true,
		clientFoundRows:             true,
		skipQueryPlanCache:          true,
		sqlSelectLimit:              true,
		transactionMode:             true,
		workload:                    true,
		version:                     true,
		versionComment:              true,
		ddlStrategy:                 true,
		sessionUUID:                 true,
		sessionEnableSystemSettings: true,
		rawGTID:                     true,
		rawTimeout:                  true,
		sessTrackGTID:               true,
		socket:                      true,
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := Parse(tc.in)
			require.NoError(err)

			result, err := RewriteAST(stmt, "ks") // passing `ks` just to test that no rewriting happens as it is not system schema
			require.NoError(err)

			expected, err := Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			s := String(expected)
			assert := assert.New(t)
			assert.Equal(s, String(result.AST))
			assert.Equal(tc.liid, result.NeedsFuncResult(LastInsertIDName), "should need last insert id")
			assert.Equal(tc.db, result.NeedsFuncResult(DBVarName), "should need database name")
			assert.Equal(tc.foundRows, result.NeedsFuncResult(FoundRowsName), "should need found rows")
			assert.Equal(tc.rowCount, result.NeedsFuncResult(RowCountName), "should need row count")
			assert.Equal(tc.udv, len(result.NeedUserDefinedVariables), "count of user defined variables")
			assert.Equal(tc.autocommit, result.NeedsSysVar(sysvars.Autocommit.Name), "should need :__vtautocommit")
			assert.Equal(tc.clientFoundRows, result.NeedsSysVar(sysvars.ClientFoundRows.Name), "should need :__vtclientFoundRows")
			assert.Equal(tc.skipQueryPlanCache, result.NeedsSysVar(sysvars.SkipQueryPlanCache.Name), "should need :__vtskipQueryPlanCache")
			assert.Equal(tc.sqlSelectLimit, result.NeedsSysVar(sysvars.SQLSelectLimit.Name), "should need :__vtsqlSelectLimit")
			assert.Equal(tc.transactionMode, result.NeedsSysVar(sysvars.TransactionMode.Name), "should need :__vttransactionMode")
			assert.Equal(tc.workload, result.NeedsSysVar(sysvars.Workload.Name), "should need :__vtworkload")
			assert.Equal(tc.ddlStrategy, result.NeedsSysVar(sysvars.DDLStrategy.Name), "should need ddlStrategy")
			assert.Equal(tc.sessionUUID, result.NeedsSysVar(sysvars.SessionUUID.Name), "should need sessionUUID")
			assert.Equal(tc.sessionEnableSystemSettings, result.NeedsSysVar(sysvars.SessionEnableSystemSettings.Name), "should need sessionEnableSystemSettings")
			assert.Equal(tc.rawGTID, result.NeedsSysVar(sysvars.ReadAfterWriteGTID.Name), "should need rawGTID")
			assert.Equal(tc.rawTimeout, result.NeedsSysVar(sysvars.ReadAfterWriteTimeOut.Name), "should need rawTimeout")
			assert.Equal(tc.sessTrackGTID, result.NeedsSysVar(sysvars.SessionTrackGTIDs.Name), "should need sessTrackGTID")
			assert.Equal(tc.version, result.NeedsSysVar(sysvars.Version.Name), "should need Vitess version")
			assert.Equal(tc.versionComment, result.NeedsSysVar(sysvars.VersionComment.Name), "should need Vitess version")
			assert.Equal(tc.socket, result.NeedsSysVar(sysvars.Socket.Name), "should need :__vtsocket")
		})
	}
}

func TestRewritesWithDefaultKeyspace(in *testing.T) {
	tests := []myTestCase{{
		in:       "SELECT 1 from x.test",
		expected: "SELECT 1 from x.test", // no change
	}, {
		in:       "SELECT x.col as c from x.test",
		expected: "SELECT x.col as c from x.test", // no change
	}, {
		in:       "SELECT 1 from test",
		expected: "SELECT 1 from sys.test",
	}, {
		in:       "SELECT 1 from test as t",
		expected: "SELECT 1 from sys.test as t",
	}, {
		in:       "SELECT 1 from `test 24` as t",
		expected: "SELECT 1 from sys.`test 24` as t",
	}, {
		in:       "SELECT 1, (select 1 from test) from x.y",
		expected: "SELECT 1, (select 1 from sys.test) from x.y",
	}, {
		in:       "SELECT 1 from (select 2 from test) t",
		expected: "SELECT 1 from (select 2 from sys.test) t",
	}, {
		in:       "SELECT 1 from test where exists (select 2 from test)",
		expected: "SELECT 1 from sys.test where exists (select 2 from sys.test)",
	}, {
		in:       "SELECT 1 from dual",
		expected: "SELECT 1 from dual",
	}, {
		in:       "SELECT (select 2 from dual) from DUAL",
		expected: "SELECT 2 as `(select 2 from dual)` from DUAL",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := Parse(tc.in)
			require.NoError(err)

			result, err := RewriteAST(stmt, "sys")
			require.NoError(err)

			expected, err := Parse(tc.expected)
			require.NoError(err, "test expectation does not parse [%s]", tc.expected)

			assert.Equal(t, String(expected), String(result.AST))
		})
	}
}

func TestRewriteToCNF(in *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{{
		in:       "not (not A = 3)",
		expected: "A = 3",
	}, {
		in:       "not (A = 3 and B = 2)",
		expected: "not A = 3 or not B = 2",
	}, {
		in:       "not (A = 3 or B = 2)",
		expected: "not A = 3 and not B = 2",
	}, {
		in:       "A xor B",
		expected: "(A or B) and not (A and B)",
	}, {
		in:       "(A and B) or C",
		expected: "(A or C) and (B or C)",
	}, {
		in:       "C or (A and B)",
		expected: "(C or A) and (C or B)",
	}, {
		in:       "A and A",
		expected: "A",
	}, {
		in:       "A OR A",
		expected: "A",
	}, {
		in:       "A OR (A AND B)",
		expected: "A",
	}, {
		in:       "A OR (B AND A)",
		expected: "A",
	}, {
		in:       "(A AND B) OR A",
		expected: "A",
	}, {
		in:       "(B AND A) OR A",
		expected: "A",
	}, {
		in:       "(A and B) and (B and A)",
		expected: "A and B",
	}, {
		in:       "(A or B) and A",
		expected: "A",
	}, {
		in:       "A and (A or B)",
		expected: "A",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			stmt, err := Parse("SELECT * FROM T WHERE " + tc.in)
			require.NoError(t, err)

			expr := stmt.(*Select).Where.Expr
			expr, didRewrite := rewriteToCNFExpr(expr)
			assert.True(t, didRewrite)
			assert.Equal(t, tc.expected, String(expr))
		})
	}
}

func TestFixedPointRewriteToCNF(in *testing.T) {
	tests := []struct {
		in       string
		expected string
	}{{
		in:       "A xor B",
		expected: "(A or B) and (not A or not B)",
	}, {
		in:       "(A and B) and (B and A) and (B and A) and (A and B)",
		expected: "A and B",
	}, {
		in:       "((A and B) OR (A and C) OR (A and D)) and E and F",
		expected: "A and ((A or B) and (B or C or A)) and ((A or D) and ((B or A or D) and (B or C or D))) and E and F",
	}, {
		in:       "(A and B) OR (A and C)",
		expected: "A and ((B or A) and (B or C))",
	}}

	for _, tc := range tests {
		in.Run(tc.in, func(t *testing.T) {
			require := require.New(t)
			stmt, err := Parse("SELECT * FROM T WHERE " + tc.in)
			require.NoError(err)

			expr := stmt.(*Select).Where.Expr
			output := RewriteToCNF(expr)
			assert.Equal(t, tc.expected, String(output))
		})
	}
}
