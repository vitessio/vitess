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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestVExplainMySQLNoRoutesSpecialHandling verifies that when a Route resolves to
// no shard but is marked for no-routes special handling (e.g. an aggregate SELECT
// whose predicate maps to no shard), MYSQLPLAN mirrors normal execution by falling
// back to an arbitrary shard, so EXPLAIN output is still produced for that Route.
func TestVExplainMySQLNoRoutesSpecialHandling(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	route := NewRoute(
		None,
		&vindexes.Keyspace{Name: "ks", Sharded: true},
		"dummy_select",
		"dummy_select_field",
	)
	route.Vindex = vindex.(vindexes.SingleColumn)
	route.Values = nil
	route.NoRoutesSpecialHandling = true

	vexplain := &VExplain{Input: route, Type: sqlparser.MySQLVExplainType}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("json", "varchar"), `{"plan":"x"}`)},
	}

	result, err := vexplain.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	// The no-route path must fall back to an arbitrary shard and EXPLAIN against it.
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone explain format = json dummy_select  ks -20`,
	})

	// The Route node must carry the per-shard EXPLAIN output, keyed by the
	// fallback shard.
	require.Len(t, result.Rows, 1)
	require.Contains(t, result.Rows[0][0].ToString(), "mysql_explain_json")
	require.Contains(t, result.Rows[0][0].ToString(), `"-20"`)
	require.Contains(t, result.Rows[0][0].ToString(), `"plan": "x"`)
}

// TestVExplainMySQLPushedDownLimit verifies that when a pushed-down scatter limit
// rewrites its child Route's query to use :__upper_limit, MYSQLPLAN computes that
// bind variable before running EXPLAIN, mirroring Limit.TryExecute - otherwise the
// EXPLAIN would be sent with :__upper_limit unbound.
func TestVExplainMySQLPushedDownLimit(t *testing.T) {
	route := NewRoute(
		Scatter,
		&vindexes.Keyspace{Name: "ks", Sharded: true},
		"dummy_select limit :__upper_limit",
		"dummy_select_field",
	)
	limit := &Limit{
		Count:  evalengine.NewLiteralInt(5),
		Offset: evalengine.NewLiteralInt(10),
		Input:  route,
	}

	vexplain := &VExplain{Input: limit, Type: sqlparser.MySQLVExplainType}

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("json", "varchar"), `{"plan":"x"}`),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("json", "varchar"), `{"plan":"x"}`),
		},
	}

	_, err := vexplain.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	// EXPLAIN must be sent to every shard with :__upper_limit bound to count+offset.
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteStandalone explain format = json dummy_select limit :__upper_limit __upper_limit: type:INT64 value:"15" ks -20`,
		`ExecuteStandalone explain format = json dummy_select limit :__upper_limit __upper_limit: type:INT64 value:"15" ks 20-`,
	})
}

// TestVExplainMySQLReservedConn verifies that MYSQLPLAN fails closed when the
// session holds a reserved connection (e.g. one that created a temporary table):
// each EXPLAIN would run on a separate standalone connection that cannot see the
// session's temporary tables, so we reject rather than report a misleading plan.
func TestVExplainMySQLReservedConn(t *testing.T) {
	route := NewRoute(
		Scatter,
		&vindexes.Keyspace{Name: "ks", Sharded: true},
		"dummy_select",
		"dummy_select_field",
	)

	vexplain := &VExplain{Input: route, Type: sqlparser.MySQLVExplainType}

	vc := &loggingVCursor{
		shards:         []string{"-20", "20-"},
		inReservedConn: true,
		results:        []*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("json", "varchar"), `{"plan":"x"}`)},
	}

	_, err := vexplain.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, false)
	require.ErrorContains(t, err, "VEXPLAIN MYSQLPLAN is not supported in a session that holds a reserved connection")
	// It must fail before touching any shard: no destinations resolved, no EXPLAIN sent.
	vc.ExpectLog(t, nil)
}
