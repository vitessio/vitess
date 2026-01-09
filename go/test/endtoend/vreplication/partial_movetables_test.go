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
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/utils"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/vt/log"
)

// testCancel() starts and cancels a partial MoveTables for one of the shards which will be actually moved later on.
// Before canceling, we first switch traffic to the target keyspace and then reverse it back to the source keyspace.
// This tests that artifacts are being properly cleaned up when a MoveTables ia canceled.
func testCancel(t *testing.T) {
	sourceKeyspace := defaultTargetKs
	targetKeyspace := "customer2"
	workflowName := "partial80DashForCancel"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)
	// We use a different table in this MoveTables than the subsequent one, so that setting up of the artifacts
	// while creating MoveTables do not paper over any issues with cleaning up artifacts when MoveTables is canceled.
	// Ref: https://github.com/vitessio/vitess/issues/13998
	table := "customer2"
	shard := "80-"
	// start the partial movetables for 80-
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         table,
		sourceShards:   shard,
	}, workflowFlavorVtctld)
	mt.Create()

	checkDenyList := func(keyspace string, expected bool) {
		validateTableInDenyList(t, vc, fmt.Sprintf("%s:%s", keyspace, shard), table, expected)
	}

	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	checkDenyList(targetKeyspace, false)
	checkDenyList(sourceKeyspace, false)

	mt.SwitchReadsAndWrites()
	checkDenyList(targetKeyspace, false)
	checkDenyList(sourceKeyspace, true)
	time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)

	mt.ReverseReadsAndWrites()
	checkDenyList(targetKeyspace, true)
	checkDenyList(sourceKeyspace, false)
	time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)

	mt.Cancel()
	checkDenyList(targetKeyspace, false)
	checkDenyList(sourceKeyspace, false)
}

func testPartialMoveTablesBasic(t *testing.T, flavor workflowFlavor) {
	setSidecarDBName("_vt")
	origDefaultReplicas := defaultReplicas
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultReplicas = origDefaultReplicas
		defaultRdonly = origDefaultRdonly
	}()
	defaultReplicas = 0
	defaultRdonly = 0
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
		utils.GetFlagVariantForTests("--schema-change-signal") + "=false",
	}...)
	defer func() {
		extraVTGateArgs = origExtraVTGateArgs
	}()
	vc = setupMinimalCluster(t)
	defer vc.TearDown()
	sourceKeyspace := defaultSourceKs
	targetKeyspace := defaultTargetKs
	workflowName := "wf1"
	targetTabs := setupMinimalTargetKeyspace(t)
	targetTab80Dash := targetTabs["80-"]
	targetTabDash80 := targetTabs["-80"]
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         "customer,loadtest,customer2",
	}, flavor)
	mt.Create()

	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	catchup(t, targetTab80Dash, workflowName, "MoveTables")
	vdiff(t, targetKeyspace, workflowName, defaultCellName, nil)
	mt.SwitchReadsAndWrites()
	time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)
	mt.Complete()

	emptyGlobalRoutingRules := "{}\n"

	sourceKeyspace = defaultTargetKs
	targetKeyspace = "customer2"

	// These should be listed in shard order
	emptyShardRoutingRules := `{"rules":[]}`
	preCutoverShardRoutingRules := fmt.Sprintf(`{"rules":[{"from_keyspace":"%s","to_keyspace":"%s","shard":"-80"},{"from_keyspace":"%s","to_keyspace":"%s","shard":"80-"}]}`, targetKeyspace, sourceKeyspace, targetKeyspace, sourceKeyspace)
	halfCutoverShardRoutingRules := fmt.Sprintf(`{"rules":[{"from_keyspace":"%s","to_keyspace":"%s","shard":"-80"},{"from_keyspace":"%s","to_keyspace":"%s","shard":"80-"}]}`, targetKeyspace, sourceKeyspace, sourceKeyspace, targetKeyspace)
	postCutoverShardRoutingRules := fmt.Sprintf(`{"rules":[{"from_keyspace":"%s","to_keyspace":"%s","shard":"-80"},{"from_keyspace":"%s","to_keyspace":"%s","shard":"80-"}]}`, sourceKeyspace, targetKeyspace, sourceKeyspace, targetKeyspace)

	// Remove any manually applied shard routing rules as these
	// should be set by SwitchTraffic.
	applyShardRoutingRules(t, emptyShardRoutingRules)
	require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))

	runWithLoad := true

	// Now setup the customer2 keyspace so we can do a partial
	// move tables for one of the two shards: 80-.
	defaultRdonly = 0
	setupCustomer2Keyspace(t)
	testCancel(t)

	// We specify the --shards flag for one of the workflows to confirm that both the MoveTables and Workflow commands
	// work the same with or without the flag.
	workflowExecOptsPartialDash80 := &workflowExecOptions{
		deferSecondaryKeys: true,
		shardSubset:        "-80",
	}
	workflowExecOptsPartial80Dash := &workflowExecOptions{
		deferSecondaryKeys: true,
	}
	var err error
	workflowName = "partial80Dash"
	shard := "80-"
	tables := "customer,loadtest"
	mt80Dash := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         tables,
		sourceShards:   shard,
	}, flavor)
	mt80Dash.Create()

	var lg *loadGenerator
	if runWithLoad { // start load after routing rules are set, otherwise we end up with ambiguous tables
		lg = newLoadGenerator(t, vc)
		go func() {
			lg.start()
		}()
		lg.waitForCount(1000)
	}
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	catchup(t, targetTab80Dash, workflowName, "MoveTables")
	vdiff(t, targetKeyspace, workflowName, defaultCellName, nil)

	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	waitForRowCount(t, vtgateConn, sourceKeyspace, "customer", 3)        // customer: all shards
	waitForRowCount(t, vtgateConn, targetKeyspace, "customer", 3)        // customer2: all shards
	waitForRowCount(t, vtgateConn, sourceKeyspace+":80-", "customer", 2) // customer2: 80-

	confirmGlobalRoutingToSource := func() {
		output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules", "--compact")
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
	shard80DashRoutedQuery := "select name from customer where cid = 1 and noexistcol = 'foo'"
	// This query uses an ID that should always get routed to shard -80
	shardDash80RoutedQuery := "select name from customer where cid = 2 and noexistcol = 'foo'"

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
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("target: %s.80-.primary", sourceKeyspace), "Query was routed to the target before any SwitchTraffic")

	log.Infof("Testing reverse route (target->source) for shard NOT being switched")
	_, err = vtgateConn.ExecuteFetch("use `customer2:-80`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shardDash80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("target: %s.-80.primary", sourceKeyspace), "Query was routed to the target before any SwitchTraffic")

	// Switch all traffic for the shard
	mt80Dash.SwitchReadsAndWrites()
	time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)

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
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardDash80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("target: %s.-80.primary", sourceKeyspace), "Query was routed to the target before partial SwitchTraffic")

	// Shard targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2:80-`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use `%s:80-`", sourceKeyspace), 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.primary", "Query was routed to the source after partial SwitchTraffic")

	// Tablet type targeting
	_, err = vtgateConn.ExecuteFetch("use `customer2@replica`", 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardDash80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("target: %s.-80.replica", sourceKeyspace), "Query was routed to the target before partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(fmt.Sprintf("use `%s@replica`", sourceKeyspace), 0, false)
	require.NoError(t, err)
	_, err = vtgateConn.ExecuteFetch(shard80DashRoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "target: customer2.80-.replica", "Query was routed to the source after partial SwitchTraffic")
	_, err = vtgateConn.ExecuteFetch(shardDash80RoutedQuery, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("target: %s.-80.replica", sourceKeyspace), "Query was routed to the target before partial SwitchTraffic")

	workflowExec := tstWorkflowExec

	// We cannot Complete a partial move tables at the moment because
	// it will find that all traffic has (obviously) not been switched.
	err = workflowExec(t, "", workflowName, "", targetKeyspace, "", workflowActionComplete, "", "", "", workflowExecOptsPartial80Dash)
	require.Error(t, err)

	// Confirm global routing rules: -80 should still be be routed to customer
	// while 80- should be routed to customer2.
	require.Equal(t, halfCutoverShardRoutingRules, getShardRoutingRules(t))

	shard = "-80"
	workflowName = "partialDash80"
	mtDash80 := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         tables,
		sourceShards:   shard,
	}, flavor)
	mtDash80.Create()

	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())

	catchup(t, targetTabDash80, workflowName, "MoveTables")
	vdiff(t, targetKeyspace, workflowName, defaultCellName, nil)
	mtDash80.SwitchReadsAndWrites()
	time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)

	// Confirm global routing rules: everything should still be routed
	// to the source side, customer, globally.
	confirmGlobalRoutingToSource()

	// Confirm shard routing rules: all shards should be routed to the
	// target side (customer2).
	require.Equal(t, postCutoverShardRoutingRules, getShardRoutingRules(t))
	lg.stop()

	// Cancel both reverse workflows (as we've done the cutover), which should
	// clean up both the global routing rules and the shard routing rules.
	for _, wf := range []string{"partialDash80", "partial80Dash"} {
		// We switched traffic, so it's the reverse workflow we want to cancel.
		var opts *workflowExecOptions
		switch wf {
		case "partialDash80":
			opts = workflowExecOptsPartialDash80
		case "partial80Dash":
			opts = workflowExecOptsPartial80Dash
		}
		reverseWf := wf + "_reverse"
		reverseKs := sourceKeyspace
		err = workflowExec(t, "", reverseWf, "", reverseKs, "", workflowActionCancel, "", "", "", opts)
		require.NoError(t, err)

		output, err := vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", reverseKs, "show", "--workflow", reverseWf, "--shards", opts.shardSubset)
		require.NoError(t, err)
		require.True(t, isEmptyWorkflowShowOutput(output))

		// Be sure we've deleted the original workflow.
		_, _ = vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", targetKeyspace, "delete", "--workflow", wf, "--shards", opts.shardSubset)
		output, err = vc.VtctldClient.ExecuteCommandWithOutput("Workflow", "--keyspace", targetKeyspace, "show", "--workflow", wf, "--shards", opts.shardSubset)
		require.NoError(t, err, output)
		require.True(t, isEmptyWorkflowShowOutput(output))
	}

	// Confirm that the global routing rules are now gone.
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetRoutingRules", "--compact")
	require.NoError(t, err)
	require.Equal(t, emptyGlobalRoutingRules, output)

	// Confirm that the shard routing rules are now gone.
	require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))
}

// TestPartialMoveTablesBasic tests partial move tables by moving each
// customer shard -- -80,80- -- once a a time to customer2.
func TestPartialMoveTablesBasic(t *testing.T) {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	testPartialMoveTablesBasic(t, workflowFlavorVtctld)
}
