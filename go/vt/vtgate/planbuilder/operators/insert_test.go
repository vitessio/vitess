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

package operators

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestModifyForAutoinc(t *testing.T) {
	var testCases []string = []string{
		"INSERT INTO seqtest (customer_id, order_date, total_amount, string) VALUES (11, '2023-09-02', 200.00);",
		"INSERT INTO seqtest (customer_id, order_date, total_amount) VALUES (11, '2023-09-02', 200.00, 'test);",
	}
	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc)
			require.NoError(t, err)

			insert := ast.(*sqlparser.Insert)
			_, err = semantics.Analyze(insert, "", &semantics.FakeSI{})
			require.NoError(t, err)

			ctx := &plancontext.PlanningContext{SemTable: semantics.EmptySemTable()}

			tableInfo, qt, err := createQueryTableForDML(ctx, insert.Table, nil)
			require.NoError(t, err)

			vindexTable, _, err := buildVindexTableForDML(ctx, tableInfo, qt, "insert")
			require.NoError(t, err)

			modifyInc, err := modifyForAutoinc(insert, vindexTable)

			require.Equal(t, modifyInc, nil)
			require.Equal(t, err, vterrors.VT03006())
		})
	}
}