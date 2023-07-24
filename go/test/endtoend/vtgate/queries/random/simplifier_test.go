/*
Copyright 2023 The Vitess Authors.

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

package random

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
	utils2 "vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/simplifier"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSimplifyResultsMismatchedQuery(t *testing.T) {
	// t.Skip("Skip CI")
	query := "select /*vt+ PLANNER=Gen4 */ distinct 'opossum' and count(*) as caggr0, case count('mustang') > -36 when min(18 * -41) then 22 - max(tbl0.sal) else 7 end as caggr1 from emp as tbl0 where case when false then tbl0.ename when 17 then 'gator' else 'mite' end and case false and true when 'worm' then tbl0.job when tbl0.ename then case when true then tbl0.ename when false then 'squirrel' end end"
	simplified := simplifyResultsMismatchedQuery(t, query)

	var err error
	t.Helper()
	t.Run(simplified, func(t *testing.T) {
		mcmp, closer := start(t)
		defer closer()

		_, err = mcmp.ExecAllowAndCompareError(simplified)
	})

	require.ErrorContains(t, err, "mismatched")

}

// TODO: suppress output from comparing intermediate simplified results
// given a query that errors with results mismatched, simplifyResultsMismatchedQuery returns a simpler version with the same error
func simplifyResultsMismatchedQuery(t *testing.T, query string) string {
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	var err error
	t.Helper()
	t.Run(query, func(t *testing.T) {
		mcmp, closer := start(t)
		defer closer()

		_, err = mcmp.ExecAllowAndCompareError(query)
		if err == nil {
			t.Fatalf("query (%s) does not error", query)
		} else if !strings.Contains(err.Error(), "mismatched") {
			t.Fatalf("query (%s) does not error with results mismatched\nError: %v", query, err)
		}
	})

	formal, err := vindexes.LoadFormal("svschema.json")
	require.NoError(t, err)
	vSchema := vindexes.BuildVSchema(formal)
	vSchemaWrapper := &utils2.VSchemaWrapper{
		V:       vSchema,
		Version: planbuilder.Gen4,
	}

	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)

	simplified := simplifier.SimplifyStatement(
		stmt.(sqlparser.SelectStatement),
		vSchemaWrapper.CurrentDb(),
		vSchemaWrapper,
		func(statement sqlparser.SelectStatement) (sameError bool) {
			q := sqlparser.String(statement)
			t.Helper()
			t.Run(q, func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()

				_, newErr := mcmp.ExecAllowAndCompareError(q)
				if newErr == nil {
					sameError = false
				} else {
					sameError = strings.Contains(newErr.Error(), "mismatched")
				}
			})
			return
		},
	)

	return sqlparser.String(simplified)
}
