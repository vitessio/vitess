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

	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtgate/simplifier"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestSimplifyBuggyQuery should be used to whenever we get a planner bug reported
// It will try to minimize the query to make it easier to understand and work with the bug.
func TestSimplifyBuggyQuery(t *testing.T) {
	query := "(select id from unsharded union select id from unsharded_auto) union (select id from user union select name from unsharded)"
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "vschemas/schema.json", true),
		version: Gen4,
	}
	stmt, reserved, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	rewritten, _ := sqlparser.RewriteAST(sqlparser.CloneStatement(stmt), vschema.currentDb(), sqlparser.SQLSelectLimitUnset, "", nil, nil)
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)

	simplified := simplifier.SimplifyStatement(
		stmt.(sqlparser.SelectStatement),
		vschema.currentDb(),
		vschema,
		keepSameError(query, reservedVars, vschema, rewritten.BindVarNeeds),
	)

	fmt.Println(sqlparser.String(simplified))
}

func TestSimplifyPanic(t *testing.T) {
	t.Skip("not needed to run")
	query := "(select id from unsharded union select id from unsharded_auto) union (select id from unsharded_auto union select name from unsharded)"
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "vschemas/schema.json", true),
		version: Gen4,
	}
	stmt, reserved, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	rewritten, _ := sqlparser.RewriteAST(sqlparser.CloneStatement(stmt), vschema.currentDb(), sqlparser.SQLSelectLimitUnset, "", nil, nil)
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)

	simplified := simplifier.SimplifyStatement(
		stmt.(sqlparser.SelectStatement),
		vschema.currentDb(),
		vschema,
		keepPanicking(query, reservedVars, vschema, rewritten.BindVarNeeds),
	)

	fmt.Println(sqlparser.String(simplified))
}

func TestUnsupportedFile(t *testing.T) {
	t.Skip("run manually to see if any queries can be simplified")
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "vschemas/schema.json", true),
		version: Gen4,
	}
	fmt.Println(vschema)
	for _, tcase := range readJSONTests("unsupported_cases.txt") {
		t.Run(tcase.Query, func(t *testing.T) {
			log.Errorf("unsupported_cases.txt - %s", tcase.Query)
			stmt, reserved, err := sqlparser.Parse2(tcase.Query)
			require.NoError(t, err)
			_, ok := stmt.(sqlparser.SelectStatement)
			if !ok {
				t.Skip()
				return
			}
			rewritten, err := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset, "", nil, nil)
			if err != nil {
				t.Skip()
			}
			vschema.currentDb()

			reservedVars := sqlparser.NewReservedVars("vtg", reserved)
			ast := rewritten.AST
			origQuery := sqlparser.String(ast)
			stmt, _, _ = sqlparser.Parse2(tcase.Query)
			simplified := simplifier.SimplifyStatement(
				stmt.(sqlparser.SelectStatement),
				vschema.currentDb(),
				vschema,
				keepSameError(tcase.Query, reservedVars, vschema, rewritten.BindVarNeeds),
			)

			if simplified == nil {
				t.Skip()
			}

			simpleQuery := sqlparser.String(simplified)
			fmt.Println(simpleQuery)

			assert.Equal(t, origQuery, simpleQuery)
		})
	}
}

func keepSameError(query string, reservedVars *sqlparser.ReservedVars, vschema *vschemaWrapper, needs *sqlparser.BindVarNeeds) func(statement sqlparser.SelectStatement) bool {
	stmt, _, err := sqlparser.Parse2(query)
	if err != nil {
		panic(err)
	}
	rewritten, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset, "", nil, nil)
	ast := rewritten.AST
	_, expected := BuildFromStmt(query, ast, reservedVars, vschema, rewritten.BindVarNeeds, true, true)
	if expected == nil {
		panic("query does not fail to plan")
	}
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

func keepPanicking(query string, reservedVars *sqlparser.ReservedVars, vschema *vschemaWrapper, needs *sqlparser.BindVarNeeds) func(statement sqlparser.SelectStatement) bool {
	cmp := func(statement sqlparser.SelectStatement) (res bool) {
		defer func() {
			r := recover()
			if r != nil {
				log.Errorf("panicked with %v", r)
				res = true
			}
		}()
		log.Errorf("trying %s", sqlparser.String(statement))
		_, _ = BuildFromStmt(query, statement, reservedVars, vschema, needs, true, true)
		log.Errorf("did not panic")

		return false
	}

	stmt, _, err := sqlparser.Parse2(query)
	if err != nil {
		panic(err.Error())
	}
	if !cmp(stmt.(sqlparser.SelectStatement)) {
		panic("query is not panicking")
	}

	return cmp
}
