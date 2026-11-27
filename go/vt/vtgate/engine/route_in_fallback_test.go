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

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type changingINVCursor struct {
	*loggingVCursor
	evaluations  int
	routedValues []*querypb.Value
}

// DATABASE() provides a deterministic changing expression for the regression;
// the production symptom also occurs with genuinely nondeterministic UUID().
func (vc *changingINVCursor) GetKeyspace() string {
	vc.evaluations++
	return fmt.Sprintf("evaluation-%d", vc.evaluations)
}

func (vc *changingINVCursor) ResolveDestinations(ctx context.Context, keyspace string, ids []*querypb.Value, destinations []key.ShardDestination) ([]*srvtopo.ResolvedShard, [][]*querypb.Value, error) {
	if len(ids) > 0 {
		vc.routedValues = ids
	}
	return vc.loggingVCursor.ResolveDestinations(ctx, keyspace, ids, destinations)
}

// A lookup-miss fallback must use the tuple checked by the lookup, rather than
// evaluating a changing expression again. Reusing the Route must not reuse a
// previous execution's tuple or modify the caller's bind variables.
func TestINLookupMissReusesEvaluatedTuple(t *testing.T) {
	for _, expression := range []string{"database()", "uuid()"} {
		for _, mode := range []string{"buffered", "streaming", "explain"} {
			t.Run(expression+"/"+mode, func(t *testing.T) {
				vc := &changingINVCursor{loggingVCursor: &loggingVCursor{shards: []string{"-20", "20-"}}}
				ast, err := sqlparser.NewTestParser().ParseExpr("(" + expression + ", 'a')")
				require.NoError(t, err)
				expr, err := evalengine.Translate(ast, &evalengine.Config{
					Environment: vc.Environment(), Collation: vc.ConnCollation(), NoConstantFolding: true,
				})
				require.NoError(t, err)
				vindex, err := vindexes.CreateVindex("lookup_unicodeloosemd5_hash_unique", "", map[string]string{
					"table": "lkp", "from": "from", "to": "toc",
				})
				require.NoError(t, err)
				route := NewRoute(IN, &vindexes.Keyspace{Name: "ks", Sharded: true},
					"select id from ref union all select id from user where lookup_non_planable in ::__vals", "")
				route.Vindex = vindex
				route.Values = []evalengine.Expr{expr}
				route.NoRoutesSpecialHandling = true
				var sent map[string]*querypb.BindVariable
				vc.onExecuteMultiShardFn = func(_ context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, _, _ bool) {
					require.Len(t, queries, 1)
					sent = queries[0].BindVariables
				}
				vc.onStreamExecuteMultiFn = func(_ context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, _, _ bool, _ func(*sqltypes.Result) error) {
					require.Len(t, vars, 1)
					sent = vars[0]
				}
				bindVars := map[string]*querypb.BindVariable{"kept": sqltypes.Int64BindVariable(7)}
				for execution := range 2 {
					sent = nil
					switch mode {
					case "streaming":
						err = route.TryStreamExecute(t.Context(), vc, bindVars, false, func(*sqltypes.Result) error { return nil })
					case "explain":
						var tasks []mysqlExplainTask
						err = (&VExplain{}).collectMySQLExplainTasks(t.Context(), vc, route, bindVars, &tasks)
						require.Len(t, tasks, 1)
						require.Len(t, tasks[0].queries, 1)
						sent = tasks[0].queries[0].BindVariables
					default:
						_, err = route.TryExecute(t.Context(), vc, bindVars, false)
					}
					require.NoError(t, err)
					require.Len(t, vc.routedValues, 2)
					require.Contains(t, sent, ListVarName)
					want := &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: vc.routedValues}
					assert.True(t, proto.Equal(want, sent[ListVarName]), "fallback tuple differs from the values checked by the lookup")
					if expression == "database()" {
						assert.Equal(t, execution+1, vc.evaluations, "evaluate the tuple once per execution")
					}
					require.Len(t, bindVars, 1)
					assert.True(t, proto.Equal(sqltypes.Int64BindVariable(7), bindVars["kept"]))
				}
			})
		}
	}
}

// Composite IN routing must retain each original list under its indexed name,
// omit scalar-column lists, and evaluate scalar expressions only once too.
func TestMultiColumnINFallbackReusesEvaluatedValues(t *testing.T) {
	for _, tc := range []struct {
		name        string
		expressions []string
		wantLists   map[string]*querypb.BindVariable
	}{
		{
			name: "lists", expressions: []string{"(database(), 'a')", "(1, 2)"},
			wantLists: map[string]*querypb.BindVariable{
				"__vals0": {Type: querypb.Type_TUPLE, Values: []*querypb.Value{
					{Type: sqltypes.VarChar, Value: []byte("evaluation-1")}, {Type: sqltypes.VarChar, Value: []byte("a")},
				}},
				"__vals1": {Type: querypb.Type_TUPLE, Values: []*querypb.Value{
					{Type: sqltypes.Int64, Value: []byte("1")}, {Type: sqltypes.Int64, Value: []byte("2")},
				}},
			},
		},
		{
			name: "scalar-first", expressions: []string{"database()", "(1, 2)"},
			wantLists: map[string]*querypb.BindVariable{
				"__vals1": {Type: querypb.Type_TUPLE, Values: []*querypb.Value{
					{Type: sqltypes.Int64, Value: []byte("1")}, {Type: sqltypes.Int64, Value: []byte("2")},
				}},
			},
		},
		{
			name: "scalar-last", expressions: []string{"(1, 2)", "database()"},
			wantLists: map[string]*querypb.BindVariable{
				"__vals0": {Type: querypb.Type_TUPLE, Values: []*querypb.Value{
					{Type: sqltypes.Int64, Value: []byte("1")}, {Type: sqltypes.Int64, Value: []byte("2")},
				}},
			},
		},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				vc := &changingINVCursor{loggingVCursor: &loggingVCursor{shards: []string{"-20", "20-"}}}
				vindex, err := vindexes.CreateVindex("multicol", "", map[string]string{"column_count": "2", "column_vindex": "hash,hash"})
				require.NoError(t, err)
				route := NewRoute(IN, &vindexes.Keyspace{Name: "ks", Sharded: true}, "dummy_select", "")
				route.Vindex = vindex
				route.NoRoutesSpecialHandling = true
				for _, expression := range tc.expressions {
					ast, err := sqlparser.NewTestParser().ParseExpr(expression)
					require.NoError(t, err)
					expr, err := evalengine.Translate(ast, &evalengine.Config{
						Environment: vc.Environment(), Collation: vc.ConnCollation(), NoConstantFolding: true,
					})
					require.NoError(t, err)
					route.Values = append(route.Values, expr)
				}
				var sent map[string]*querypb.BindVariable
				vc.onExecuteMultiShardFn = func(_ context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, queries []*querypb.BoundQuery, _, _ bool) {
					require.Len(t, queries, 1)
					sent = queries[0].BindVariables
				}
				vc.onStreamExecuteMultiFn = func(_ context.Context, _ Primitive, _ string, _ []*srvtopo.ResolvedShard, vars []map[string]*querypb.BindVariable, _, _ bool, _ func(*sqltypes.Result) error) {
					require.Len(t, vars, 1)
					sent = vars[0]
				}
				bindVars := map[string]*querypb.BindVariable{"kept": sqltypes.Int64BindVariable(7)}
				if stream {
					err = route.TryStreamExecute(t.Context(), vc, bindVars, false, func(*sqltypes.Result) error { return nil })
				} else {
					_, err = route.TryExecute(t.Context(), vc, bindVars, false)
				}
				require.NoError(t, err)
				assert.Equal(t, 1, vc.evaluations)
				require.Len(t, sent, len(tc.wantLists)+1)
				for name, want := range tc.wantLists {
					require.Contains(t, sent, name)
					assert.True(t, proto.Equal(want, sent[name]), name)
				}
				assert.True(t, proto.Equal(sqltypes.Int64BindVariable(7), sent["kept"]))
				require.Len(t, bindVars, 1)
			})
		}
	}
}
