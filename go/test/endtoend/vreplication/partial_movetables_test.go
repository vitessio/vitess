/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

// TestPartialMoveTables tests partial move tables by moving each
// customer shard -- -80,80- -- once a a time to customer2.
func TestPartialMoveTables(t *testing.T) {
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 1
	origExtraVTGateArgs := extraVTGateArgs
	// We need to enable shard routing for partial movetables routing.
	// And we need to disable schema change tracking in vtgate as we want
	// to test query routing using a query we know will fail as it's
	// using a column that doesn't exist in the schema -- this way we
	// get the target shard details back in the error message. If schema
	// tracking is enabled then vtgate will produce an error about the
	// unknown symbol before attempting to route the query.
	extraVTGateArgs = append(extraVTGateArgs, []string{
		"--enable-partial-keyspace-migration",
		"--schema_change_signal=false",
	}...)
	defer func() {
		extraVTGateArgs = origExtraVTGateArgs
	}()
	vc = setupCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown(t)
	setupCustomerKeyspace(t)

	// Move customer table from unsharded product keyspace to
	// sharded customer keyspace.
	createMoveTablesWorkflow(t, "customer")
	tstWorkflowSwitchReadsAndWrites(t)
	tstWorkflowComplete(t)

	emptyGlobalRoutingRules := "{}\n"

	// These should be listed in shard order
	emptyShardRoutingRules := `{"rules":[]}`
	preCutoverShardRoutingRules := `{"rules":[{"from_keyspace":"customer2","to_keyspace":"customer","shard":"-80"},{"from_keyspace":"customer2","to_keyspace":"customer","shard":"80-"}]}`
	halfCutoverShardRoutingRules := `{"rules":[{"from_keyspace":"customer2","to_keyspace":"customer","shard":"-80"},{"from_keyspace":"customer","to_keyspace":"customer2","shard":"80-"}]}`
	postCutoverShardRoutingRules := `{"rules":[{"from_keyspace":"customer","to_keyspace":"customer2","shard":"-80"},{"from_keyspace":"customer","to_keyspace":"customer2","shard":"80-"}]}`

	// Remove any manually applied shard routing rules as these
	// should be set by SwitchTraffic.
	applyShardRoutingRules(t, emptyShardRoutingRules)
	require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))

	// Now setup the customer2 keyspace so we can do a partial
	// move tables for one of the two shards: 80-.
	defaultRdonly = 0
	setupCustomer2Keyspace(t)
	currentWorkflowType = wrangler.MoveTablesWorkflow
	wfName := "partial80Dash"
	sourceKs := "customer"
	targetKs := "customer2"
	shard := "80-"
	ksWf := fmt.Sprintf("%s.%s", targetKs, wfName)

	// start the partial movetables for 80-
	err := tstWorkflowExec(t, defaultCellName, wfName, sourceKs, targetKs,
		"customer", workflowActionCreate, "", shard, "")
	require.NoError(t, err)
	targetTab1 = vc.getPrimaryTablet(t, targetKs, shard)
	catchup(t, targetTab1, wfName, "Partial MoveTables Customer to Customer2")
	vdiff1(t, ksWf, "")

	waitForRowCount(t, vtgateConn, "customer", "customer", 3)      // customer: all shards
	waitForRowCount(t, vtgateConn, "customer2", "customer", 3)     // customer2: all shards
	waitForRowCount(t, vtgateConn, "customer2:80-", "customer", 2) // customer2: 80-

	confirmGlobalRoutingToSource := func() {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("GetRoutingRules")
		require.NoError(t, err)
		result := gjson.Get(output, "rules")
		result.ForEach(func(attributeKey, attributeValue gjson.Result) bool {
			// 0 is the keyspace and 1 is optional tablename[@tablettype]
			fromKsTbl := strings.Split(attributeValue.Get("fromTable").String(), ".")
			// 0 is the keyspace and 1 is the tablename
			toKsTbl := strings.Split(attributeValue.Get("toTables.0").String(), ".")
			// All tables in the customer and customer2 keyspaces should be
			// routed to the customer keyspace.
			if fromKsTbl[0] == "customer" || fromKsTbl[0] == "customer2" {
				require.Equal(t, "customer", toKsTbl[0])
			}
			return true
		})
	}

	// This query uses an ID that should always get routed to shard 80-
	shard80MinusRoutedQuery := "select name from customer where cid = 1 and noexistcol = 'foo'"
	// This query uses an ID that should always get routed to shard -80
	shardMinus80RoutedQuery := "select name from customer where cid = 2 and noexistcol = 'foo'"

	// reset any existing vtgate connection state
	vtgateConn.Close()
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	// Global routing rules should be in place with everything going to
	// the source keyspace (customer).
	confirmGlobalRoutingToSource()

	// Shard routing rules should now also be in place with everything
	// going to the source keyspace (customer).
	require.Equal(t, preCutoverShardRoutingRules, getShardRoutingRules(t))

	// Confirm shard targeting works before we switch any traffic.
	// Everything should be routed to the source keyspace (customer).

	log.Infof("Testing reverse route (target->source) for shard being switched")
	_, err = vtgateConn.ExecuteFetch("use `customer2:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.80-.primary", "Query was routed to the target before any SwitchTraffic")

	log.Infof("Testing reverse route (target->source) for shard NOT being switched")
	_, err = vtgateConn.ExecuteFetch("use `customer2:-80`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shardMinus80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.primary", "Query was routed to the target before any SwitchTraffic")

	// Switch all traffic for the shard
	require.NoError(t, tstWorkflowExec(t, "", wfName, "", targetKs, "", workflowActionSwitchTraffic, "", "", ""))
	expectedSwitchOutput := fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s\nStart State: Reads Not Switched. Writes Not Switched\nCurrent State: Reads partially switched, for shards: %s. Writes partially switched, for shards: %s\n\n",
		targetKs, wfName, shard, shard)
	require.Equal(t, expectedSwitchOutput, lastOutput)

	// Confirm global routing rules -- everything should still be routed
	// to the source side, customer, globally.
	confirmGlobalRoutingToSource()

	// Confirm shard routing rules -- all traffic for the 80- shard should be
	// routed into the customer2 keyspace, overriding the global routing rules.
	require.Equal(t, halfCutoverShardRoutingRules, getShardRoutingRules(t))

	// reset any existing vtgate connection state
	vtgateConn.Close()
	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	// No shard targeting
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardMinus80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.primary", "Query was routed to the target before partial SwitchTraffic")

	// Shard targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch("use `customer:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")

	// Tablet type targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2@replica`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardMinus80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.replica", "Query was routed to the target before partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch("use `customer@replica`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80MinusRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardMinus80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer.-80.replica", "Query was routed to the target before partial SwitchTraffic")

	// We cannot Complete a partial move tables at the moment because
	// it will find that all traffic has (obviously) not been switched.
	err = tstWorkflowExec(t, "", wfName, "", targetKs, "", workflowActionComplete, "", "", "")
	require.Error(t, err)

	// Confirm global routing rules: -80 should still be be routed to customer
	// while 80- should be routed to customer2.
	require.Equal(t, halfCutoverShardRoutingRules, getShardRoutingRules(t))

	// Now move the other shard: -80
	wfName = "partialDash80"
	shard = "-80"
	ksWf = fmt.Sprintf("%s.%s", targetKs, wfName)

	// Start the partial movetables for -80, 80- has already been switched
	err = tstWorkflowExec(t, defaultCellName, wfName, sourceKs, targetKs,
		"customer", workflowActionCreate, "", shard, "")
	require.NoError(t, err)
	targetTab2 := vc.getPrimaryTablet(t, targetKs, shard)
	catchup(t, targetTab2, wfName, "Partial MoveTables Customer to Customer2: -80")
	vdiff1(t, ksWf, "")
	// Switch all traffic for the shard
	require.NoError(t, tstWorkflowExec(t, "", wfName, "", targetKs, "", workflowActionSwitchTraffic, "", "", ""))
	expectedSwitchOutput = fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s\nStart State: Reads partially switched, for shards: 80-. Writes partially switched, for shards: 80-\nCurrent State: All Reads Switched. All Writes Switched\n\n",
		targetKs, wfName)
	require.Equal(t, expectedSwitchOutput, lastOutput)

	// Confirm global routing rules: everything should still be routed
	// to the source side, customer, globally.
	confirmGlobalRoutingToSource()

	// Confirm shard routing rules: all shards should be routed to the
	// target side (customer2).
	require.Equal(t, postCutoverShardRoutingRules, getShardRoutingRules(t))

	// Cancel both reverse workflows (as we've done the cutover), which should
	// clean up both the global routing rules and the shard routing rules.
	for _, wf := range []string{"partialDash80", "partial80Dash"} {
		// We switched traffic, so it's the reverse workflow we want to cancel.
		reverseWf := wf + "_reverse"
		reverseKs := sourceKs // customer
		err = tstWorkflowExec(t, "", reverseWf, "", reverseKs, "", workflowActionCancel, "", "", "")
		require.NoError(t, err)

		output, err := vc.VtctlClient.ExecuteCommandWithOutput("Workflow", fmt.Sprintf("%s.%s", reverseKs, reverseWf), "show")
		require.Error(t, err)
		require.Contains(t, output, "no streams found")

		// Delete the original workflow
		originalKsWf := fmt.Sprintf("%s.%s", targetKs, wf)
		_, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", originalKsWf, "delete")
		require.NoError(t, err)
		output, err = vc.VtctlClient.ExecuteCommandWithOutput("Workflow", originalKsWf, "show")
		require.Error(t, err)
		require.Contains(t, output, "no streams found")
	}

	// Confirm that the global routing rules are now gone.
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("GetRoutingRules")
	require.NoError(t, err)
	require.Equal(t, emptyGlobalRoutingRules, output)

	// Confirm that the shard routing rules are now gone.
	require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))
}
