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

package vtgate

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/sqlparser"
)

// TestRequestDirectivesSharePlan checks that statements that differ only in
// their request-only comment directives share one VTGate plan, and that MySQL
// receives each statement with its own directive block.
func TestRequestDirectivesSharePlan(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// Capture the general log of each shard's primary for the duration of
	// the test.
	var tablets []*cluster.Vttablet
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		tablets = append(tablets, shard.PrimaryTablet())
	}
	for _, tablet := range tablets {
		for _, q := range []string{"set global log_output = 'TABLE'", "truncate mysql.general_log", "set global general_log = 'ON'"} {
			_, err := tablet.VttabletProcess.QueryTablet(q, KeyspaceName, true)
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() {
		for _, tablet := range tablets {
			for _, q := range []string{"set global general_log = default", "set global log_output = default"} {
				_, err := tablet.VttabletProcess.QueryTablet(q, KeyspaceName, true)
				assert.NoError(t, err)
			}
		}
	})

	blocks := []string{
		"/*vt+ WORKLOAD_NAME=request_directives_1 PRIORITY=10 */",
		"/*vt+ WORKLOAD_NAME=request_directives_2 PRIORITY=20 CONSOLIDATOR=disabled */",
	}
	// sharedPlans returns the plans of the two statements, by query, and all
	// plans.
	sharedPlans := func() (map[string]any, map[string]any) {
		plans, err := clusterInstance.VtgateProcess.ReadQueryPlans()
		if err != nil {
			return nil, nil
		}
		shared := map[string]any{}
		for query, plan := range plans {
			if strings.Contains(query, sqlparser.DirectiveRequestDirectives) && (strings.Contains(query, "into t1(id1, id2)") || strings.Contains(query, "id1, id2 from t1 order by id1")) {
				shared[query] = plan
			}
		}
		return shared, plans
	}

	for i, block := range blocks {
		utils.Exec(t, conn, fmt.Sprintf("insert %s into t1(id1, id2) values (%d, %d)", block, i+1, i+1))
		utils.Exec(t, conn, fmt.Sprintf("select %s id1, id2 from t1 order by id1", block))
		if i == 0 {
			// The plan cache stores plans in the background, so wait for the
			// plans of the first statements before running the others.
			require.Eventually(t, func() bool {
				shared, _ := sharedPlans()
				return len(shared) == 2
			}, 30*time.Second, 100*time.Millisecond)
		}
	}

	// The second statements ran the plans of the first ones, which hold
	// neither statement's directives.
	shared, plans := sharedPlans()
	require.Len(t, shared, 2, "plans: %v", plans)
	for query, plan := range shared {
		assert.EqualValues(t, 2, plan.(map[string]any)["ExecCount"], query)
	}
	for query := range plans {
		assert.NotContains(t, query, "request_directives_")
	}

	// The insert reached one shard and the select both, each with the
	// statement's own block and without the marker.
	var received []string
	for _, tablet := range tablets {
		qr, err := tablet.VttabletProcess.QueryTablet(
			"select argument from mysql.general_log where (argument like '%request_directives_%' or argument like '%"+sqlparser.DirectiveRequestDirectives+"%') and argument not like '%general_log%'",
			KeyspaceName, true)
		require.NoError(t, err)
		var selects []string
		for _, row := range qr.Rows {
			query := row[0].ToString()
			received = append(received, query)
			if strings.HasPrefix(query, "select ") {
				selects = append(selects, query)
			}
		}
		for _, block := range blocks {
			assert.Truef(t, containsPrefix(selects, "select "+block+" id1, id2 from t1"), "tablet %s didn't receive the select with %s: %v", tablet.Alias, block, selects)
		}
	}
	for i, block := range blocks {
		assert.Truef(t, containsPrefix(received, "insert "+block+" into t1(id1, id2) values"), "no tablet received the insert with %s: %v", block, received)
		for _, query := range received {
			assert.NotContains(t, query, sqlparser.DirectiveRequestDirectives)
			if strings.Contains(query, fmt.Sprintf("request_directives_%d", i+1)) {
				assert.Contains(t, query, block)
			}
		}
	}
}

func containsPrefix(queries []string, prefix string) bool {
	for _, query := range queries {
		if strings.HasPrefix(query, prefix) {
			return true
		}
	}
	return false
}
