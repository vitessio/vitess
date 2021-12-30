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

package planbuilder

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/simplifier"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestSimplifyUnsupportedQuery should be used to whenever we get a planner bug reported
// It will try to minimize the query to make it easier to understand and work with the bug.
func TestSimplifyUnsupportedQuery(t *testing.T) {
	query := "select user.id, user.name, count(*), unsharded.name from user join unsharded where unsharded.id = 42"
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "schema_test.json", true),
		version: Gen4,
	}
	stmt, reserved, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	rewritten, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset)
	vschema.currentDb()

	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	ast := rewritten.AST
	_, err = BuildFromStmt(query, ast, reservedVars, vschema, rewritten.BindVarNeeds, true, true)
	require.Error(t, err)

	simplified := simplifier.SimplifyStatement(
		ast.(sqlparser.SelectStatement),
		vschema.currentDb(),
		vschema,
		keepSameError(query, reservedVars, vschema, rewritten.BindVarNeeds, err),
	)

	fmt.Println(sqlparser.String(simplified))
}

func TestUnsupportedFile(t *testing.T) {
	t.Skip("run manually to see if any queries can be simplified")
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "schema_test.json", true),
		version: Gen4,
	}
	fmt.Println(vschema)
	for tcase := range iterateExecFile("unsupported_cases.txt") {
		t.Run(fmt.Sprintf("%d:%s", tcase.lineno, tcase.input), func(t *testing.T) {
			log.Errorf("%s:%d - %s", tcase.file, tcase.lineno, tcase.input)
			stmt, reserved, err := sqlparser.Parse2(tcase.input)
			require.NoError(t, err)
			_, ok := stmt.(sqlparser.SelectStatement)
			if !ok {
				t.Skip()
				return
			}
			rewritten, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset)
			vschema.currentDb()

			reservedVars := sqlparser.NewReservedVars("vtg", reserved)
			ast := rewritten.AST
			_, err = BuildFromStmt(sqlparser.String(ast), ast, reservedVars, vschema, rewritten.BindVarNeeds, true, true)

			simplified := simplifier.SimplifyStatement(
				ast.(sqlparser.SelectStatement),
				vschema.currentDb(),
				vschema,
				keepSameError(tcase.input, reservedVars, vschema, rewritten.BindVarNeeds, err),
			)

			fmt.Println(sqlparser.String(simplified))
		})
	}
}

func keepSameError(query string, reservedVars *sqlparser.ReservedVars, vschema *vschemaWrapper, needs *sqlparser.BindVarNeeds, expected error) func(statement sqlparser.SelectStatement) bool {
	return func(statement sqlparser.SelectStatement) bool {
		_, myErr := BuildFromStmt(query, statement, reservedVars, vschema, needs, true, true)
		if myErr == nil {
			return false
		}
		state := vterrors.ErrState(expected)
		if state == vterrors.Undefined {
			return expected.Error() == myErr.Error()
		}
		return vterrors.ErrState(myErr) == state
	}
}
