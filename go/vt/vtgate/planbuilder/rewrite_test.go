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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestHavingRewrite(t *testing.T) {
	tcases := []struct {
		input  string
		output string
		sqs    map[string]string
	}{{
		input:  "select 1 from t1 having a = 1",
		output: "select 1 from t1 where a = 1",
	}, {
		input:  "select 1 from t1 where x = 1 and y = 2 having a = 1",
		output: "select 1 from t1 where x = 1 and y = 2 and a = 1",
	}, {
		input:  "select 1 from t1 where x = 1 or y = 2 having a = 1",
		output: "select 1 from t1 where (x = 1 or y = 2) and a = 1",
	}, {
		input:  "select 1 from t1 where x = 1 having a = 1 and b = 2",
		output: "select 1 from t1 where x = 1 and a = 1 and b = 2",
	}, {
		input:  "select 1 from t1 where x = 1 having a = 1 or b = 2",
		output: "select 1 from t1 where x = 1 and (a = 1 or b = 2)",
	}, {
		input:  "select 1 from t1 where x = 1 and y = 2 having a = 1 and b = 2",
		output: "select 1 from t1 where x = 1 and y = 2 and a = 1 and b = 2",
	}, {
		input:  "select 1 from t1 where x = 1 or y = 2 having a = 1 and b = 2",
		output: "select 1 from t1 where (x = 1 or y = 2) and a = 1 and b = 2",
	}, {
		input:  "select 1 from t1 where x = 1 and y = 2 having a = 1 or b = 2",
		output: "select 1 from t1 where x = 1 and y = 2 and (a = 1 or b = 2)",
	}, {
		input:  "select 1 from t1 where x = 1 or y = 2 having a = 1 or b = 2",
		output: "select 1 from t1 where (x = 1 or y = 2) and (a = 1 or b = 2)",
	}, {
		input:  "select 1 from t1 where x = 1 or y = 2 having a = 1 and count(*) = 1",
		output: "select 1 from t1 where (x = 1 or y = 2) and a = 1 having count(*) = 1",
	}, {
		input:  "select count(*) k from t1 where x = 1 or y = 2 having a = 1 and k = 1",
		output: "select count(*) as k from t1 where (x = 1 or y = 2) and a = 1 having count(*) = 1",
	}, {
		input:  "select count(*) k from t1 having k = 10",
		output: "select count(*) as k from t1 having count(*) = 10",
	}, {
		input:  "select 1 from t1 group by a having a = 1 and count(*) > 1",
		output: "select 1 from t1 where a = 1 group by a having count(*) > 1",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			semTable, reservedVars, sel := prepTest(t, tcase.input)
			err := queryRewrite(semTable, reservedVars, sel)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, sqlparser.String(sel))
		})
	}
}

func prepTest(t *testing.T, sql string) (*semantics.SemTable, *sqlparser.ReservedVars, *sqlparser.Select) {
	ast, vars, err := sqlparser.NewTestParser().Parse2(sql)
	require.NoError(t, err)

	sel, isSelectStatement := ast.(*sqlparser.Select)
	require.True(t, isSelectStatement, "analyzer expects a select statement")

	reservedVars := sqlparser.NewReservedVars("vtg", vars)
	semTable, err := semantics.Analyze(sel, "", &semantics.FakeSI{})
	require.NoError(t, err)

	return semTable, reservedVars, sel
}
