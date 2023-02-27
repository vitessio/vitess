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

func TestSubqueryRewrite(t *testing.T) {
	tcases := []struct {
		input  string
		output string
	}{{
		input:  "select 1 from t1",
		output: "select 1 from t1",
	}, {
		input:  "select (select 1) from t1",
		output: "select :__sq1 from t1",
	}, {
		input:  "select 1 from t1 where exists (select 1)",
		output: "select 1 from t1 where :__sq_has_values1",
	}, {
		input:  "select id from t1 where id in (select 1)",
		output: "select id from t1 where :__sq_has_values1 = 1 and id in ::__sq1",
	}, {
		input:  "select id from t1 where id not in (select 1)",
		output: "select id from t1 where :__sq_has_values1 = 0 or id not in ::__sq1",
	}, {
		input:  "select id from t1 where id = (select 1)",
		output: "select id from t1 where id = :__sq1",
	}, {
		input:  "select id from t1 where id >= (select 1)",
		output: "select id from t1 where id >= :__sq1",
	}, {
		input:  "select id from t1 where t1.id = (select 1 from t2 where t2.id = t1.id)",
		output: "select id from t1 where t1.id = :__sq1",
	}, {
		input:  "select id from t1 join t2 where t1.id = t2.id and exists (select 1)",
		output: "select id from t1 join t2 where t1.id = t2.id and :__sq_has_values1",
	}, {
		input:  "select id from t1 where not exists (select 1)",
		output: "select id from t1 where not :__sq_has_values1",
	}, {
		input:  "select id from t1 where not exists (select 1) and exists (select 2)",
		output: "select id from t1 where not :__sq_has_values1 and :__sq_has_values2",
	}, {
		input:  "select (select 1), (select 2) from t1 join t2 on t1.id = (select 1) where t1.id in (select 1)",
		output: "select :__sq2, :__sq3 from t1 join t2 on t1.id = :__sq1 where :__sq_has_values4 = 1 and t1.id in ::__sq4",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			ast, vars, err := sqlparser.Parse2(tcase.input)
			require.NoError(t, err)
			reservedVars := sqlparser.NewReservedVars("vtg", vars)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			semTable, err := semantics.Analyze(selectStatement, "", &semantics.FakeSI{})
			require.NoError(t, err)
			err = queryRewrite(semTable, reservedVars, selectStatement)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, sqlparser.String(selectStatement))
		})
	}
}

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
		input:  "select 1 from t1 where x in (select 1 from t2 having a = 1)",
		output: "select 1 from t1 where :__sq_has_values1 = 1 and x in ::__sq1",
		sqs:    map[string]string{"__sq1": "select 1 from t2 where a = 1"},
	}, {input: "select 1 from t1 group by a having a = 1 and count(*) > 1",
		output: "select 1 from t1 where a = 1 group by a having count(*) > 1",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			semTable, reservedVars, sel := prepTest(t, tcase.input)
			err := queryRewrite(semTable, reservedVars, sel)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, sqlparser.String(sel))
			squeries, found := semTable.SubqueryMap[sel]
			if len(tcase.sqs) > 0 {
				assert.True(t, found, "no subquery found in the query")
				assert.Equal(t, len(tcase.sqs), len(squeries), "number of subqueries not matched")
			}
			for _, sq := range squeries {
				assert.Equal(t, tcase.sqs[sq.GetArgName()], sqlparser.String(sq.Subquery.Select))
			}
		})
	}
}

func prepTest(t *testing.T, sql string) (*semantics.SemTable, *sqlparser.ReservedVars, *sqlparser.Select) {
	ast, vars, err := sqlparser.Parse2(sql)
	require.NoError(t, err)

	sel, isSelectStatement := ast.(*sqlparser.Select)
	require.True(t, isSelectStatement, "analyzer expects a select statement")

	reservedVars := sqlparser.NewReservedVars("vtg", vars)
	semTable, err := semantics.Analyze(sel, "", &semantics.FakeSI{})
	require.NoError(t, err)

	return semTable, reservedVars, sel
}
