/*
Copyright 2022 The Vitess Authors.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyErrorAndComments(t *testing.T) {
	testcases := []struct {
		input  string
		output string
		err    error
	}{
		{
			input:  "select 1",
			output: "select 1 from dual",
		}, {
			input: "",
			err:   ErrEmpty,
		}, {
			input: ";",
			err:   ErrEmpty,
		}, {
			input:  "-- sdf",
			output: "-- sdf",
		}, {
			input:  "/* sdf */",
			output: "/* sdf */",
		}, {
			input:  "# sdf",
			output: "# sdf",
		}, {
			input:  "/* sdf */ select 1",
			output: "select 1 from dual",
		},
	}
	parser := NewTestParser()
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			res, err := parser.Parse(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})

		t.Run(testcase.input+"-Strict DDL", func(t *testing.T) {
			res, err := parser.ParseStrictDDL(testcase.input)
			if testcase.err != nil {
				require.Equal(t, testcase.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testcase.output, String(res))
			}
		})
	}
}

func TestSplitStatementToPieces(t *testing.T) {
	testcases := []struct {
		input     string
		output    string
		lenWanted int
	}{{
		input:  "select * from table1; \t; \n; \n\t\t ;select * from table1;",
		output: "select * from table1;select * from table1",
	}, {
		input: "select * from table",
	}, {
		input:  "select * from table;",
		output: "select * from table",
	}, {
		input:  "select * from table1;   ",
		output: "select * from table1",
	}, {
		input:  "select * from table1; select * from table2;",
		output: "select * from table1; select * from table2",
	}, {
		input:  "select * from /* comment ; */ table1;",
		output: "select * from /* comment ; */ table1",
	}, {
		input:  "select * from table where semi = ';';",
		output: "select * from table where semi = ';'",
	}, {
		input:  "select * from table1;--comment;\nselect * from table2;",
		output: "select * from table1;--comment;\nselect * from table2",
	}, {
		input: "CREATE TABLE `total_data` (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id', " +
			"`region` varchar(32) NOT NULL COMMENT 'region name, like zh; th; kepler'," +
			"`data_size` bigint NOT NULL DEFAULT '0' COMMENT 'data size;'," +
			"`createtime` datetime NOT NULL DEFAULT NOW() COMMENT 'create time;'," +
			"`comment` varchar(100) NOT NULL DEFAULT '' COMMENT 'comment'," +
			"PRIMARY KEY (`id`))",
	}, {
		input:  "create table t1 (id int primary key); create table t2 (id int primary key);",
		output: "create table t1 (id int primary key); create table t2 (id int primary key)",
	}, {
		input:  ";;; create table t1 (id int primary key);;; ;create table t2 (id int primary key);",
		output: " create table t1 (id int primary key);create table t2 (id int primary key)",
	}, {
		// The input doesn't have to be valid SQL statements!
		input:  ";create table t1 ;create table t2 (id;",
		output: "create table t1 ;create table t2 (id",
	}, {
		// Ignore quoted semicolon
		input:  ";create table t1 ';';;;create table t2 (id;",
		output: "create table t1 ';';create table t2 (id",
	}, {
		// Ignore quoted semicolon
		input:  "stop replica; start replica",
		output: "stop replica; start replica",
	}, {
		// Test that we don't split on semicolons inside create procedure calls.
		input:     "create procedure p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end",
		lenWanted: 1,
	}, {
		// Test that we don't split on semicolons inside create procedure calls.
		input:     "select * from t1;create procedure p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end;select * from t2",
		lenWanted: 3,
	}, {
		// Create procedure with comments.
		input:     "select * from t1; /* comment1 */ create /* comment2 */ procedure /* comment3 */ p1 (in country CHAR(3), out cities INT) begin select count(*) from x where d = e; end;select * from t2",
		lenWanted: 3,
	}, {
		// Create procedure with definer current_user.
		input:     "create DEFINER=CURRENT_USER procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	}, {
		// Create procedure with definer current_user().
		input:     "create DEFINER=CURRENT_USER() procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	}, {
		// Create procedure with definer string.
		input:     "create DEFINER='root' procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	}, {
		// Create procedure with definer string at_id.
		input:     "create DEFINER='root'@localhost procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	}, {
		// Create procedure with definer id.
		input:     "create DEFINER=`root` procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	}, {
		// Create procedure with definer id at_id.
		input:     "create DEFINER=`root`@`localhost` procedure p1 (in country CHAR(3))  begin declare abc DECIMAL(14,2); DECLARE def DECIMAL(14,2); end",
		lenWanted: 1,
	},
	}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}

			stmtPieces, err := parser.SplitStatementToPieces(tcase.input)
			require.NoError(t, err)
			if tcase.lenWanted != 0 {
				require.Equal(t, tcase.lenWanted, len(stmtPieces))
			}
			out := strings.Join(stmtPieces, ";")
			require.Equal(t, tcase.output, out)
		})
	}
}
