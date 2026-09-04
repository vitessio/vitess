/*
Copyright 2026 The Vitess Authors.

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

package endtoend

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestMultiStatementParity checks that a multi-statement query is split the
// way MySQL splits it. For every input, the number of results MySQL returns
// with CLIENT_MULTI_STATEMENTS and the error it ends with must match what
// sqlparser.Parser.ForEachStatement hands out: the same number of statements
// before the error, and an error of the same class (1064 syntax error, 1065
// empty query, or none).
//
// TODO(#20884): add the sql_mode dependent inputs (NO_BACKSLASH_ESCAPES,
// ANSI_QUOTES, and a SET sql_mode inside the batch) once the loop parses
// with the session's parser. Batches with CREATE FUNCTION, CREATE TRIGGER or
// CREATE EVENT bodies cannot be compared either: the Vitess grammar does not
// parse those statements at all.
func TestMultiStatementParity(t *testing.T) {
	inputs := []string{
		"select 1;; select 2",
		"select 1;;",
		"select 1;;;",
		"select 1 ; ;",
		"select 1;; -- c",
		"select 1;; /* c */",
		"select 1; /* c */ ;",
		"select 1; -- c\n;",
		"-- c;",
		" ;",
		"select 1;",
		"select 1;   ",
		"; select 2",
		";",
		"",
		"   ",
		"select 1; -- trailing",
		"select 1; /* c */",
		"select 1;\n;\n select 2",
		"select 1 /* ; */; select 2",
		"select 1 -- ;\n; select 2",
		"select 1 # ;\n; select 2",
		"select 1 -- ; select 2",
		"select ';'; select 2",
		"select 1 as `a;b`; select 2",
		`select 1 as ";"; select 2`,
		`select 'a\'; select 2`,
		"/*! select 1 */; select 2",
		"select 1; select 2;",
		"begin; select 1; commit",
		"select 1; bogus; select 3",
		"select 1 select 2; select 3",
		"create procedure p1() begin select 1; select 2; end; select 3",
		"create procedure p1() begin bogus; end; select 1",
		"if 1 then select 1; end if; select 2",
	}

	params := connParams
	params.Flags |= mysql.CapabilityClientMultiStatements
	parser := sqlparser.NewTestParser()
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			conn, err := mysql.Connect(t.Context(), &params)
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.ExecuteFetch("drop procedure if exists p1", 0, false)
			require.NoError(t, err)

			mysqlResults, mysqlErrno := runOnMySQL(t, conn, input)
			vitessResults, vitessErrno := runThroughForEachStatement(parser, input)

			assert.Equal(t, mysqlResults, vitessResults, "number of results before the error")
			assert.Equal(t, mysqlErrno, vitessErrno, "error class")
		})
	}
}

// runOnMySQL returns the number of results MySQL returned for the batch and
// the number of the error that ended it, 0 for none.
func runOnMySQL(t *testing.T, conn *mysql.Conn, sql string) (results int, errno sqlerror.ErrorCode) {
	_, more, err := conn.ExecuteFetchMulti(sql, 100, true)
	for err == nil {
		results++
		if !more {
			return results, 0
		}
		_, more, _, err = conn.ReadQueryResult(100, true)
	}
	var sqlErr *sqlerror.SQLError
	require.ErrorAs(t, err, &sqlErr)
	return results, sqlErr.Num
}

// runThroughForEachStatement plays the batch the way vtgate does: each
// statement ForEachStatement hands out is parsed, as the executor would, and
// the first error ends the batch. It returns the number of statements that
// parsed before the error and the MySQL error number the error maps to
// (syntax errors of the grammar count as ER_PARSE_ERROR), 0 for none.
func runThroughForEachStatement(parser *sqlparser.Parser, sql string) (results int, errno sqlerror.ErrorCode) {
	err := parser.ForEachStatement(sql, func(text, _ string) error {
		if _, err := parser.Parse(text); err != nil {
			return err
		}
		results++
		return nil
	})
	if err == nil {
		return results, 0
	}
	var sqlErr *sqlerror.SQLError
	if errors.As(sqlerror.NewSQLErrorFromError(err), &sqlErr) && sqlErr.Num != sqlerror.ERUnknownError {
		return results, sqlErr.Num
	}
	if vterrors.Code(err) == vtrpcpb.Code_INVALID_ARGUMENT {
		return results, sqlerror.ERParseError
	}
	return results, sqlerror.ERUnknownError
}
