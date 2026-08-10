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
