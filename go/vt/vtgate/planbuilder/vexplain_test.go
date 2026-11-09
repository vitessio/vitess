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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestCheckVExplainMySQLASTPrioritizesUnsafeFunctions proves that when a query
// contains an advisory lock function, the rejection MYSQLPLAN reports must be the
// lock rejection - which deliberately never points the user at VEXPLAIN ALL - even
// when another rejected construct (a subquery/derived table, whose message does
// recommend VEXPLAIN ALL) is also present. Recommending VEXPLAIN ALL for a query
// containing get_lock would execute it, acquiring the advisory lock: exactly the
// side effect the lock rejection guards against.
func TestCheckVExplainMySQLASTPrioritizesUnsafeFunctions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		query   string
		wantErr string
	}{
		{
			// Sibling: get_lock is visited first, then the sibling subquery
			// overwrites astErr with the recommend-VEXPLAIN-ALL message.
			name:    "lock function beside a subquery",
			query:   `select get_lock('x', 1), (select 1)`,
			wantErr: vexplainMySQLLockError,
		},
		{
			// Nested: the outer subquery is rejected and its children pruned, so
			// the inner get_lock is never visited at all.
			name:    "lock function nested inside a subquery",
			query:   `select (select get_lock('x', 1))`,
			wantErr: vexplainMySQLLockError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tc.query)
			require.NoError(t, err)

			err = checkVExplainMySQLAST(stmt)
			require.ErrorContains(t, err, tc.wantErr)
			require.NotContains(t, err.Error(), "VEXPLAIN ALL",
				"a query containing an unsafe function must not be steered to VEXPLAIN ALL, which would execute it")
		})
	}
}
