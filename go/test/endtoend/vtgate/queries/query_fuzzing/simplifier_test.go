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

package query_fuzzing

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder"
	"vitess.io/vitess/go/vt/vtgate/simplifier"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestSimplifyResultsMismatchedQuery(t *testing.T) {
	t.Skip("Skip CI")

	// This test minimizes failing queries to their simplest form where the results still mismatch between the databases.
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "emp", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, keyspaceName, "dept", clusterInstance.VtgateProcess.ReadVSchema))

	queries := []string{
		"select (0 & 0) div min(tbl0.deptno) from emp as tbl0",
	}

	for _, query := range queries {
		var simplified string
		t.Run("simplification "+query, func(t *testing.T) {
			simplified = simplifyResultsMismatchedQuery(t, query)
		})

		t.Run("simplified "+query, func(t *testing.T) {
			mcmp, closer := start(t)
			defer closer()

			mcmp.ExecAllowAndCompareError(simplified)
		})

		fmt.Printf("final simplified query: %s\n", simplified)
	}
}

// given a query that errors with results mismatched, simplifyResultsMismatchedQuery returns a simpler version with the same error
func simplifyResultsMismatchedQuery(t *testing.T, query string) string {
	t.Helper()
	mcmp, closer := start(t)
	defer closer()

	_, err := mcmp.ExecAllowAndCompareError(query)
	require.ErrorContainsf(t, err, "mismatched", "query (%s) does not error with results mismatched", query)

	formal, err := vindexes.LoadFormal("svschema.json")
	require.NoError(t, err)
	vSchema := vindexes.BuildVSchema(formal)
	vSchemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:       vSchema,
		Version: planbuilder.Gen4,
	}

	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)

	simplified := simplifier.SimplifyStatement(
		stmt.(sqlparser.SelectStatement),
		vSchemaWrapper.CurrentDb(),
		vSchemaWrapper,
		func(statement sqlparser.SelectStatement) bool {
			q := sqlparser.String(statement)
			_, newErr := mcmp.ExecAllowAndCompareError(q)
			if newErr == nil {
				return false
			} else {
				return strings.Contains(newErr.Error(), "mismatched")
			}
		},
	)

	return sqlparser.String(simplified)
}
