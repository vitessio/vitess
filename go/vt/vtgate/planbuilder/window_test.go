/*
Copyright 2025 The Vitess Authors.

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

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestWindowFunctions(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)

	tests := []struct {
		name          string
		sql           string
		isSingleShard bool
	}{
		// Single Shard Cases (Should Pass)
		{
			name:          "Single Shard - Rank",
			sql:           "select rank() over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - RowNumber",
			sql:           "select row_number() over () from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - DenseRank",
			sql:           "select dense_rank() over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Sum Partition By",
			sql:           "select sum(col) over (partition by textcol1) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Avg Partition By Order By Rows",
			sql:           "select avg(col) over (partition by textcol1 order by col rows between 1 preceding and 1 following) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Lead",
			sql:           "select lead(col, 1) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Lag",
			sql:           "select lag(col, 1) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - FirstValue",
			sql:           "select first_value(col) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - LastValue",
			sql:           "select last_value(col) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - NthValue",
			sql:           "select nth_value(col, 2) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Ntile",
			sql:           "select ntile(4) over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Multiple Windows",
			sql:           "select rank() over (order by col), row_number() over (partition by textcol1) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Range Frame",
			sql:           "select count(*) over (order by col range between unbounded preceding and current row) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Named Window",
			sql:           "select rank() over w from user where Id = 1 window w as (order by col)",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Window in ORDER BY",
			sql:           "select col from user where Id = 1 order by rank() over (order by col)",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Window with GROUP BY",
			sql:           "select col, count(*) from user where Id = 1 group by col order by rank() over (order by count(*))",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Window with DISTINCT",
			sql:           "select distinct col, rank() over (order by col) from user where Id = 1",
			isSingleShard: true,
		},
		{
			name:          "Single Shard - Window with LIMIT",
			sql:           "select rank() over (order by col) from user where Id = 1 limit 5",
			isSingleShard: true,
		},
		{
			name:          "Unsharded Table - Rank",
			sql:           "select rank() over (order by predef1) from unsharded",
			isSingleShard: true,
		},
		{
			name:          "Authoritative Table - Partition By Primary Vindex",
			sql:           "select rank() over (partition by user_id) from authoritative",
			isSingleShard: true,
		},
		{
			name:          "Authoritative Table - Partition By Primary Vindex and Valid Column",
			sql:           "select rank() over (partition by user_id, col1) from authoritative",
			isSingleShard: true,
		},
		{
			name:          "Authoritative Table - Partition By Primary Vindex and Invalid Column",
			sql:           "select rank() over (partition by user_id, invalid_col) from authoritative",
			isSingleShard: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, ctx, err := buildOperator(t, tt.sql, vw)
			require.NoError(t, err)

			var windowOp *operators.Window
			_ = operators.Visit(op, func(o operators.Operator) error {
				if w, ok := o.(*operators.Window); ok {
					windowOp = w
				}
				return nil
			})

			if tt.isSingleShard {
				// Verify it can be transformed to primitive without error
				_, err = transformToPrimitive(ctx, op)
				require.NoError(t, err, "Should transform to primitive successfully")
			} else {
				// For scatter, Window operator should be present initially
				assert.NotNil(t, windowOp, "Window operator should be present for scatter query")

				// But transformation should fail
				_, err = transformToPrimitive(ctx, op)
				require.Error(t, err)
				require.Contains(t, err.Error(), "window functions are only supported for single-shard queries")
			}
		})
	}
}

func buildOperator(t *testing.T, sql string, vschema plancontext.VSchema) (operators.Operator, *plancontext.PlanningContext, error) {
	stmt, _, err := vschema.Environment().Parser().Parse2(sql)
	require.NoError(t, err)

	selStmt := stmt.(sqlparser.SelectStatement)
	reservedVars := sqlparser.NewReservedVars("vtg", nil)
	ctx, err := plancontext.CreatePlanningContext(selStmt, reservedVars, vschema, query.ExecuteOptions_Gen4)
	require.NoError(t, err)

	op, err := operators.PlanQuery(ctx, selStmt)
	return op, ctx, err
}
