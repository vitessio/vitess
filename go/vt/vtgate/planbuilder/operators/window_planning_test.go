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

package operators

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestHasWindowFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "regular query without window functions",
			sql:      "SELECT id, name FROM users",
			expected: false,
		},
		{
			name:     "query with ROW_NUMBER window function",
			sql:      "SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders",
			expected: true,
		},
		{
			name:     "salary ranking within departments using named windows",
			sql:      "SELECT employee_id, salary, RANK() OVER w FROM employees WINDOW w AS (PARTITION BY dept_id ORDER BY salary DESC)",
			expected: true,
		},
		{
			name:     "analytics query with multiple window functions for user metrics",
			sql:      "SELECT user_id, score, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY score DESC) as user_rank, PERCENT_RANK() OVER (ORDER BY score) as global_percentile FROM user_scores",
			expected: true,
		},
		{
			name:     "top-N per group query pattern with window functions",
			sql:      "SELECT * FROM (SELECT id, user_id, amount, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) as rn FROM orders) t WHERE rn <= 3",
			expected: true,
		},
		{
			name:     "running totals with SUM window function",
			sql:      "SELECT order_id, user_id, amount, SUM(amount) OVER (PARTITION BY user_id ORDER BY order_date) as running_total FROM orders",
			expected: true,
		},
		{
			name:     "non-SELECT statement should return false",
			sql:      "INSERT INTO users (id, name) VALUES (1, 'test')",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.sql)
			assert.NoError(t, err)

			ctx := &plancontext.PlanningContext{
				Statement: stmt,
			}

			result := hasWindowFunctions(ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateWindowSpec(t *testing.T) {
	tests := []struct {
		name            string
		spec            *sqlparser.WindowSpecification
		shardingColumns []string
		expectError     bool
	}{
		{
			name: "window function with single sharding key in partition - should pass",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("user_id")},
				},
			},
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
		{
			name: "window function with composite vindex - all columns present - should pass",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("tenant_id")},
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("user_id")},
				},
			},
			shardingColumns: []string{"tenant_id", "user_id"},
			expectError:     false,
		},
		{
			name: "window function with composite vindex - missing column - should fail",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("user_id")},
				},
			},
			shardingColumns: []string{"tenant_id", "user_id"},
			expectError:     true,
		},
		{
			name: "window function without partition clause - should fail for multi-shard",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{},
			},
			shardingColumns: []string{"user_id"},
			expectError:     true,
		},
		{
			name:            "nil window spec should fail",
			spec:            nil,
			shardingColumns: []string{"user_id"},
			expectError:     true,
		},
		{
			name: "case insensitive column matching should work",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("USER_ID")},
				},
			},
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
		{
			name: "unsharded table should pass validation without partition requirements",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{},
			},
			shardingColumns: []string{},
			expectError:     false,
		},
		{
			name: "window function with expression and required column should validate",
			spec: &sqlparser.WindowSpecification{
				PartitionClause: []sqlparser.Expr{
					&sqlparser.FuncExpr{Name: sqlparser.NewIdentifierCI("YEAR")}, // Function expression like YEAR(created_at)
					&sqlparser.ColName{Name: sqlparser.NewIdentifierCI("user_id")},
				},
			},
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWindowSpec(tt.spec, tt.shardingColumns)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCanExecuteWindowsOnRoute(t *testing.T) {
	tests := []struct {
		name     string
		route    *Route
		expected bool
	}{
		{
			name: "information schema queries allow window functions",
			route: &Route{
				Routing: &InfoSchemaRouting{},
			},
			expected: true,
		},
		{
			name: "unsharded route allows window functions",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Unsharded,
				},
			},
			expected: true,
		},
		{
			name: "equal unique opcode allows window functions",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.EqualUnique,
				},
			},
			expected: true,
		},
		{
			name: "IN opcode allows window functions",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.IN,
				},
			},
			expected: true,
		},
		{
			name: "DBA route allows window functions",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.DBA,
				},
			},
			expected: true,
		},
		{
			name:     "nil route should return false",
			route:    nil,
			expected: false,
		},
		{
			name: "sharded routing with no selected vindex should return false",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Scatter,
					Selected:    nil,
				},
			},
			expected: false,
		},
		{
			name: "sharded routing with non-unique vindex should return false",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Scatter,
					Selected: &VindexOption{
						FoundVindex: &vindexes.LookupHash{}, // LookupHash is non-unique
					},
				},
			},
			expected: false,
		},
		{
			name: "sharded routing with unique vindex should return true",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Scatter,
					Selected: &VindexOption{
						FoundVindex: &vindexes.Hash{}, // Hash is unique
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := canExecuteWindowsOnRoute(tt.route)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindShardingColumns(t *testing.T) {
	hashVindex := &vindexes.Hash{}
	binaryVindex := &vindexes.Hash{} // Use another Hash instance for composite test

	tests := []struct {
		name     string
		route    *Route
		expected []string
	}{
		{
			name: "information_schema queries have no sharding requirements",
			route: &Route{
				Routing: &InfoSchemaRouting{},
			},
			expected: nil,
		},
		{
			name: "scatter queries without vindexes cannot determine sharding columns",
			route: &Route{
				Routing: &ShardedRouting{
					Selected: nil,
				},
			},
			expected: nil,
		},
		{
			name: "user-sharded table with single column vindex",
			route: &Route{
				Routing: &ShardedRouting{
					Selected: &VindexOption{
						FoundVindex: hashVindex,
					},
					VindexPreds: []*VindexPlusPredicates{
						{
							ColVindex: &vindexes.ColumnVindex{
								Vindex: hashVindex,
								Columns: []sqlparser.IdentifierCI{
									sqlparser.NewIdentifierCI("user_id"),
								},
							},
						},
					},
				},
			},
			expected: []string{"user_id"},
		},
		{
			name: "multi-tenant table with composite vindex (tenant_id, user_id)",
			route: &Route{
				Routing: &ShardedRouting{
					Selected: &VindexOption{
						FoundVindex: binaryVindex,
					},
					VindexPreds: []*VindexPlusPredicates{
						{
							ColVindex: &vindexes.ColumnVindex{
								Vindex: hashVindex,
								Columns: []sqlparser.IdentifierCI{
									sqlparser.NewIdentifierCI("user_id"),
								},
							},
						},
						{
							ColVindex: &vindexes.ColumnVindex{
								Vindex: binaryVindex,
								Columns: []sqlparser.IdentifierCI{
									sqlparser.NewIdentifierCI("tenant_id"),
									sqlparser.NewIdentifierCI("user_id"),
								},
							},
						},
					},
				},
			},
			expected: []string{"tenant_id", "user_id"},
		},
		{
			name: "query using different vindex than selected returns nil",
			route: &Route{
				Routing: &ShardedRouting{
					Selected: &VindexOption{
						FoundVindex: &vindexes.Hash{}, // Different instance
					},
					VindexPreds: []*VindexPlusPredicates{
						{
							ColVindex: &vindexes.ColumnVindex{
								Vindex: hashVindex, // Different instance
								Columns: []sqlparser.IdentifierCI{
									sqlparser.NewIdentifierCI("user_id"),
								},
							},
						},
					},
				},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findShardingColumns(tt.route)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateWindowPartitionBy(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		shardingColumns []string
		expectError     bool
	}{
		{
			name:            "window function with correct sharding key in partition",
			sql:             "SELECT ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders",
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
		{
			name:            "window function partitioned by wrong column fails validation",
			sql:             "SELECT ROW_NUMBER() OVER (PARTITION BY status ORDER BY created_at) FROM orders",
			shardingColumns: []string{"user_id"},
			expectError:     true,
		},
		{
			name:            "comparison with previous values using LAG window function",
			sql:             "SELECT user_id, score, LAG(score, 1) OVER (PARTITION BY user_id ORDER BY created_at) as prev_score FROM user_scores",
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
		{
			name:            "no window function should pass",
			sql:             "SELECT id, name FROM orders",
			shardingColumns: []string{"user_id"},
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.sql)
			assert.NoError(t, err)

			selectStmt := stmt.(*sqlparser.Select)
			err = validateWindowPartitionBy(selectStmt.SelectExprs.Exprs[0], tt.shardingColumns)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRouteForWindows(t *testing.T) {
	tests := []struct {
		name        string
		route       *Route
		expectError bool
	}{
		{
			name: "valid unsharded route",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Unsharded,
				},
			},
			expectError: false,
		},
		{
			name: "invalid scatter route without unique vindex",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Scatter,
					Selected:    nil,
				},
			},
			expectError: true,
		},
		{
			name: "valid scatter route with unique vindex",
			route: &Route{
				Routing: &ShardedRouting{
					RouteOpCode: engine.Scatter,
					Selected: &VindexOption{
						FoundVindex: &vindexes.Hash{}, // Hash is unique
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			semTable := semantics.EmptySemTable()
			// Set up the error that should be returned for multi-shard window functions
			if tt.expectError {
				semTable.NotSingleShardErr = errors.New("window functions not supported on multi-shard queries")
			}

			ctx := &plancontext.PlanningContext{
				Statement: &sqlparser.Select{},
				SemTable:  semTable,
			}
			err := validateRouteForWindows(ctx, tt.route)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
