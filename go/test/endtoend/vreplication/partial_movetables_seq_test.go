/*
Copyright 2023 The Vitess Authors.

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

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

/*
	This file introduces a new helper framework for vreplication tests. The current one uses a lot of globals
	and make assumptions which make adding new types of tests difficult.

	As part of a separate cleanup we will build on this framework to replace the existing one.
*/

type keyspace struct {
	name    string
	vschema string
	schema  string
	baseID  int64
	shards  []string
}

type workflowOptions struct {
	tables       []string
	sourceShards []string
	targetShards []string
}

type workflow struct {
	name         string
	fromKeyspace string
	toKeyspace   string
	typ          string
	tables       []string
	tc           *vrepTestCase
	options      *workflowOptions
}

type vrepTestCase struct {
	testName        string
	t               *testing.T
	cellNames       []string
	defaultCellName string
	vtgateConn      *mysql.Conn
	keyspaces       map[string]*keyspace
	workflows       map[string]*workflow

	vc     *VitessCluster
	vtgate *cluster.VtgateProcess
}

func initPartialMoveTablesComplexTestCase(t *testing.T, name string) *vrepTestCase {
	const (
		seqVSchema = `{
			"sharded": false,
			"tables": {
				"customer_seq": {
					"type": "sequence"
				}
			}
		}`
		seqSchema       = `create table customer_seq(id int, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';`
		commerceSchema  = `create table customer(cid int, name varchar(128), ts timestamp(3) not null default current_timestamp(3), primary key(cid));`
		commerceVSchema = `
		{
		  "tables": {
			"customer": {}
		  }
		}
`
		customerSchema  = ""
		customerVSchema = `
		{
		  "sharded": true,
		  "vindexes": {
			"reverse_bits": {
			  "type": "reverse_bits"
			}
		  },
		  "tables": {
			"customer": {
			  "column_vindexes": [
				{
				  "column": "cid",
				  "name": "reverse_bits"
				}
			  ],
			  "auto_increment": {
				"column": "cid",
				"sequence": "customer_seq"
			  }
			}
		  }
		}
		`
	)
	tc := &vrepTestCase{
		t:               t,
		testName:        name,
		keyspaces:       make(map[string]*keyspace),
		defaultCellName: "zone1",
		workflows:       make(map[string]*workflow),
	}
	tc.keyspaces["commerce"] = &keyspace{
		name:    "commerce",
		vschema: commerceVSchema,
		schema:  commerceSchema,
		baseID:  100,
		shards:  []string{"0"},
	}
	tc.keyspaces["customer"] = &keyspace{
		name:    "customer",
		vschema: customerVSchema,
		schema:  customerSchema,
		baseID:  200,
		shards:  []string{"-80", "80-"},
	}
	tc.keyspaces["customer2"] = &keyspace{
		name:    "customer2",
		vschema: customerVSchema,
		schema:  "",
		baseID:  1200,
		shards:  []string{"-80", "80-"},
	}
	tc.keyspaces["seqSrc"] = &keyspace{
		name:    "seqSrc",
		vschema: seqVSchema,
		schema:  seqSchema,
		baseID:  400,
		shards:  []string{"0"},
	}
	tc.keyspaces["seqTgt"] = &keyspace{
		name:    "seqTgt",
		vschema: "",
		schema:  "",
		baseID:  500,
		shards:  []string{"0"},
	}
	tc.setupCluster()
	tc.initData()
	return tc
}

func (tc *vrepTestCase) teardown() {
	tc.vtgateConn.Close()
	vc.TearDown(tc.t)
}

func (tc *vrepTestCase) setupCluster() {
	cells := []string{"zone1"}

	tc.vc = NewVitessCluster(tc.t, tc.testName, cells, mainClusterConfig)
	vc = tc.vc // for backward compatibility since vc is used globally in this package
	require.NotNil(tc.t, tc.vc)
	tc.setupKeyspaces([]string{"commerce", "seqSrc"})
	tc.vtgateConn = getConnection(tc.t, tc.vc.ClusterConfig.hostname, tc.vc.ClusterConfig.vtgateMySQLPort)
	vtgateConn = tc.vtgateConn // for backward compatibility since vtgateConn is used globally in this package
}

func (tc *vrepTestCase) initData() {
	_, err := tc.vtgateConn.ExecuteFetch("insert into customer_seq(id, next_id, cache) values(0, 1000, 1000)", 1000, false)
	require.NoError(tc.t, err)
	_, err = tc.vtgateConn.ExecuteFetch("insert into customer(cid, name) values(1, 'customer1'), (2, 'customer2'),(3, 'customer3')", 1000, false)
	require.NoError(tc.t, err)
}

func (tc *vrepTestCase) setupKeyspaces(keyspaces []string) {
	for _, keyspace := range keyspaces {
		ks, ok := tc.keyspaces[keyspace]
		require.Equal(tc.t, true, ok, "keyspace %s not found", keyspace)
		tc.setupKeyspace(ks)
	}
}

func (tc *vrepTestCase) setupKeyspace(ks *keyspace) {
	t := tc.t
	if _, err := tc.vc.AddKeyspace(t, []*Cell{tc.vc.Cells["zone1"]}, ks.name, strings.Join(ks.shards, ","),
		ks.vschema, ks.schema, 0, 0, int(ks.baseID), nil); err != nil {
		t.Fatal(err)
	}
	if tc.vtgate == nil {
		defaultCellName := "zone1"
		defaultCell := tc.vc.Cells[defaultCellName]
		require.NotNil(tc.t, defaultCell)
		tc.vtgate = defaultCell.Vtgates[0]

	}
	for _, shard := range ks.shards {
		require.NoError(t, cluster.WaitForHealthyShard(tc.vc.VtctldClient, ks.name, shard))
		require.NoError(t, tc.vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", ks.name, shard), 1, 30*time.Second))
	}
}

func (tc *vrepTestCase) newWorkflow(typ, workflowName, fromKeyspace, toKeyspace string, options *workflowOptions) *workflow {
	wf := &workflow{
		name:         workflowName,
		fromKeyspace: fromKeyspace,
		toKeyspace:   toKeyspace,
		typ:          typ,
		tc:           tc,
		options:      options,
	}
	return wf
}

func (wf *workflow) create() {
	var err error
	t := wf.tc.t
	typ := strings.ToLower(wf.typ)
	cell := wf.tc.defaultCellName
	switch typ {
	case "movetables":
		currentWorkflowType = wrangler.MoveTablesWorkflow
		sourceShards := strings.Join(wf.options.sourceShards, ",")
		err = tstWorkflowExec(t, cell, wf.name, wf.fromKeyspace, wf.toKeyspace,
			strings.Join(wf.options.tables, ","), workflowActionCreate, "", sourceShards, "")
	case "reshard":
		currentWorkflowType = wrangler.ReshardWorkflow
		sourceShards := strings.Join(wf.options.sourceShards, ",")
		targetShards := strings.Join(wf.options.targetShards, ",")
		if targetShards == "" {
			targetShards = sourceShards
		}
		err = tstWorkflowExec(t, cell, wf.name, wf.fromKeyspace, wf.toKeyspace,
			strings.Join(wf.options.tables, ","), workflowActionCreate, "", sourceShards, targetShards)
	default:
		panic(fmt.Sprintf("unknown workflow type: %s", wf.typ))
	}
	require.NoError(t, err)
	waitForWorkflowState(t, wf.tc.vc, fmt.Sprintf("%s.%s", wf.toKeyspace, wf.name), workflowStateRunning)
	ks2 := wf.tc.vc.Cells[cell].Keyspaces[wf.toKeyspace]
	var i int64
	for _, shardName := range wf.tc.keyspaces[wf.toKeyspace].shards {
		tab := ks2.Shards[shardName].Tablets[fmt.Sprintf("%s-%d", cell, wf.tc.keyspaces[wf.toKeyspace].baseID+i)].Vttablet
		catchup(t, tab, wf.name, wf.typ)
		i += 100
	}
	doVdiff2(t, wf.toKeyspace, wf.name, cell, nil)

}

func (wf *workflow) switchTraffic() {
	require.NoError(wf.tc.t, tstWorkflowExec(wf.tc.t, wf.tc.defaultCellName, wf.name, wf.fromKeyspace, wf.toKeyspace, "", workflowActionSwitchTraffic, "", "", ""))
}

func (wf *workflow) reverseTraffic() {
	require.NoError(wf.tc.t, tstWorkflowExec(wf.tc.t, wf.tc.defaultCellName, wf.name, wf.fromKeyspace, wf.toKeyspace, "", workflowActionReverseTraffic, "", "", ""))
}

func (wf *workflow) complete() {
	require.NoError(wf.tc.t, tstWorkflowExec(wf.tc.t, wf.tc.defaultCellName, wf.name, wf.fromKeyspace, wf.toKeyspace, "", workflowActionComplete, "", "", ""))
}

// TestPartialMoveTablesWithSequences enhances TestPartialMoveTables by adding an unsharded keyspace which has a
// sequence. This tests that the sequence is migrated correctly and that we can reverse traffic back to the source
func TestPartialMoveTablesWithSequences(t *testing.T) {

	origExtraVTGateArgs := extraVTGateArgs

	extraVTGateArgs = append(extraVTGateArgs, []string{
		"--enable-partial-keyspace-migration",
		"--schema_change_signal=false",
	}...)
	defer func() {
		extraVTGateArgs = origExtraVTGateArgs
	}()

	tc := initPartialMoveTablesComplexTestCase(t, "TestPartialMoveTablesComplex")
	defer tc.teardown()
	var err error

	t.Run("Move customer table from unsharded product keyspace to sharded customer keyspace.", func(t *testing.T) {
		tc.setupKeyspaces([]string{"customer"})
		wf := tc.newWorkflow("MoveTables", "customer", "commerce", "customer", &workflowOptions{
			tables: []string{"customer"},
		})
		wf.create()
		wf.switchTraffic()
		wf.complete()
	})

	var wfSeq *workflow
	t.Run("Start MoveTables for Sequence", func(t *testing.T) {
		tc.setupKeyspace(tc.keyspaces["seqTgt"])
		wfSeq = tc.newWorkflow("MoveTables", "seq", "seqSrc", "seqTgt", &workflowOptions{
			tables: []string{"customer_seq"},
		})
		wfSeq.create()
	})

	var emptyGlobalRoutingRules, emptyShardRoutingRules, preCutoverShardRoutingRules, halfCutoverShardRoutingRules, postCutoverShardRoutingRules string
	t.Run("Define and setup RoutingRules", func(t *testing.T) {
		emptyGlobalRoutingRules = "{}\n"

		// These should be listed in shard order
		emptyShardRoutingRules = `{"rules":[]}`
		preCutoverShardRoutingRules = `{"rules":[{"from_keyspace":"customer2","to_keyspace":"customer","shard":"-80"},{"from_keyspace":"customer2","to_keyspace":"customer","shard":"80-"}]}`
		halfCutoverShardRoutingRules = `{"rules":[{"from_keyspace":"customer2","to_keyspace":"customer","shard":"-80"},{"from_keyspace":"customer","to_keyspace":"customer2","shard":"80-"}]}`
		postCutoverShardRoutingRules = `{"rules":[{"from_keyspace":"customer","to_keyspace":"customer2","shard":"-80"},{"from_keyspace":"customer","to_keyspace":"customer2","shard":"80-"}]}`

		// Remove any manually applied shard routing rules as these
		// should be set by SwitchTraffic.
		applyShardRoutingRules(t, emptyShardRoutingRules)
		require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))
	})

	wfName := "partial80Dash"
	sourceKs := "customer"
	targetKs := "customer2"
	shard := "80-"
	var wf80Dash, wfDash80 *workflow
	currentCustomerCount = getCustomerCount(t, "before customer2.80-")
	t.Run("Start MoveTables on customer2.80-", func(t *testing.T) {
		// Now setup the customer2 keyspace so we can do a partial move tables for one of the two shards: 80-.
		defaultRdonly = 0
		tc.setupKeyspaces([]string{"customer2"})
		wf80Dash = tc.newWorkflow("MoveTables", wfName, "customer", "customer2", &workflowOptions{
			sourceShards: []string{"80-"},
			tables:       []string{"customer"},
		})
		wf80Dash.create()

		currentCustomerCount = getCustomerCount(t, "after customer2.80-")
		waitForRowCount(t, vtgateConn, "customer2:80-", "customer", 2) // customer2: 80-
		waitForRowCount(t, vtgateConn, "customer", "customer", 3)      // customer: all shards
		waitForRowCount(t, vtgateConn, "customer2", "customer", 3)     // customer2: all shards
	})

	currentCustomerCount = getCustomerCount(t, "after customer2.80-/2")
	log.Flush()

	// This query uses an ID that should always get routed to shard 80-
	shard80MinusRoutedQuery := "select name from customer where cid = 1 and noexistcol = 'foo'"
	// This query uses an ID that should always get routed to shard -80
	shardMinus80RoutedQuery := "select name from customer where cid = 2 and noexistcol = 'foo'"

	// Reset any existing vtgate connection state.
	vtgateConn.Close()
	vtgateConn = getConnection(t, tc.vc.ClusterConfig.hostname, tc.vc.ClusterConfig.vtgateMySQLPort)
	t.Run("Confirm routing rules", func(t *testing.T) {

		// Global routing rules should be in place with everything going to the source keyspace (customer).
		confirmGlobalRoutingToSource(t)

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

		_, err = vtgateConn.ExecuteFetch("use `customer`", 0, false) // switch vtgate default db back to customer
		require.NoError(t, err)
		currentCustomerCount = getCustomerCount(t, "")

		// Switch all traffic for the shard
		wf80Dash.switchTraffic()
		expectedSwitchOutput := fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s\nStart State: Reads Not Switched. Writes Not Switched\nCurrent State: Reads partially switched, for shards: %s. Writes partially switched, for shards: %s\n\n",
			targetKs, wfName, shard, shard)
		require.Equal(t, expectedSwitchOutput, lastOutput)
		currentCustomerCount = getCustomerCount(t, "")

		// Confirm global routing rules -- everything should still be routed
		// to the source side, customer, globally.
		confirmGlobalRoutingToSource(t)

		// Confirm shard routing rules -- all traffic for the 80- shard should be
		// routed into the customer2 keyspace, overriding the global routing rules.
		require.Equal(t, halfCutoverShardRoutingRules, getShardRoutingRules(t))

		// Confirm global routing rules: -80 should still be be routed to customer
		// while 80- should be routed to customer2.
		require.Equal(t, halfCutoverShardRoutingRules, getShardRoutingRules(t))
	})
	vtgateConn.Close()
	vtgateConn = getConnection(t, tc.vc.ClusterConfig.hostname, tc.vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()

	t.Run("Validate shard and tablet type routing", func(t *testing.T) {

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

		_, err = vtgateConn.ExecuteFetch("use `customer`", 0, false) // switch vtgate default db back to customer
		require.NoError(t, err)
	})
	currentCustomerCount = getCustomerCount(t, "")

	// Now move the other shard: -80
	t.Run("Move shard -80 and validate routing rules", func(t *testing.T) {
		// Now move the other shard: -80
		wfName = "partialDash80"
		shard = "-80"
		wfDash80 = tc.newWorkflow("MoveTables", wfName, "customer", "customer2", &workflowOptions{
			sourceShards: []string{"-80"},
			tables:       []string{"customer"},
		})
		wfDash80.create()
		wfDash80.switchTraffic()

		expectedSwitchOutput := fmt.Sprintf("SwitchTraffic was successful for workflow %s.%s\nStart State: Reads partially switched, for shards: 80-. Writes partially switched, for shards: 80-\nCurrent State: All Reads Switched. All Writes Switched\n\n",
			targetKs, wfName)
		require.Equal(t, expectedSwitchOutput, lastOutput)

		// Confirm global routing rules: everything should still be routed
		// to the source side, customer, globally.
		confirmGlobalRoutingToSource(t)

		// Confirm shard routing rules: all shards should be routed to the
		// target side (customer2).
		require.Equal(t, postCutoverShardRoutingRules, getShardRoutingRules(t))
	})

	var output string

	_, err = vtgateConn.ExecuteFetch("use `customer`", 0, false) // switch vtgate default db back to customer
	require.NoError(t, err)
	currentCustomerCount = getCustomerCount(t, "")
	t.Run("Switch sequence traffic forward and reverse and validate workflows still exist and sequence routing works", func(t *testing.T) {
		wfSeq.switchTraffic()
		log.Infof("SwitchTraffic was successful for workflow seqTgt.seq, with output %s", lastOutput)

		insertCustomers(t)

		wfSeq.reverseTraffic()
		log.Infof("ReverseTraffic was successful for workflow seqTgt.seq, with output %s", lastOutput)

		insertCustomers(t)

		wfSeq.switchTraffic()
		log.Infof("SwitchTraffic was successful for workflow seqTgt.seq, with output %s", lastOutput)

		insertCustomers(t)

		output, err = tc.vc.VtctlClient.ExecuteCommandWithOutput("Workflow", "seqTgt.seq", "show")
		require.NoError(t, err)

		output, err = tc.vc.VtctlClient.ExecuteCommandWithOutput("Workflow", "seqSrc.seq_reverse", "show")
		require.NoError(t, err)

		wfSeq.complete()
	})

	t.Run("Cancel reverse workflows and validate", func(t *testing.T) {
		// Cancel both reverse workflows (as we've done the cutover), which should
		// clean up both the global routing rules and the shard routing rules.
		for _, wf := range []string{"partialDash80", "partial80Dash"} {
			// We switched traffic, so it's the reverse workflow we want to cancel.
			reverseWf := wf + "_reverse"
			reverseKs := sourceKs // customer
			err = tstWorkflowExec(t, "", reverseWf, "", reverseKs, "", workflowActionCancel, "", "", "")
			require.NoError(t, err)

			output, err := tc.vc.VtctlClient.ExecuteCommandWithOutput("Workflow", fmt.Sprintf("%s.%s", reverseKs, reverseWf), "show")
			require.Error(t, err)
			require.Contains(t, output, "no streams found")

			// Delete the original workflow
			originalKsWf := fmt.Sprintf("%s.%s", targetKs, wf)
			_, err = tc.vc.VtctlClient.ExecuteCommandWithOutput("Workflow", originalKsWf, "delete")
			require.NoError(t, err)
			output, err = tc.vc.VtctlClient.ExecuteCommandWithOutput("Workflow", originalKsWf, "show")
			require.Error(t, err)
			require.Contains(t, output, "no streams found")
		}

		// Confirm that the global routing rules are now gone.
		output, err = tc.vc.VtctlClient.ExecuteCommandWithOutput("GetRoutingRules")
		require.NoError(t, err)
		require.Equal(t, emptyGlobalRoutingRules, output)

		// Confirm that the shard routing rules are now gone.
		require.Equal(t, emptyShardRoutingRules, getShardRoutingRules(t))
	})
}

var customerCount int64
var currentCustomerCount int64
var newCustomerCount = int64(201)
var lastCustomerId int64

func getCustomerCount(t *testing.T, msg string) int64 {
	qr := execVtgateQuery(t, vtgateConn, "", "select count(*) from customer")
	require.NotNil(t, qr)
	count, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)
	return count
}

func confirmLastCustomerIdHasIncreased(t *testing.T) {
	qr := execVtgateQuery(t, vtgateConn, "", "select cid from customer order by cid desc limit 1")
	require.NotNil(t, qr)
	currentCustomerId, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)
	require.Greater(t, currentCustomerId, lastCustomerId)
	lastCustomerId = currentCustomerId
}

func insertCustomers(t *testing.T) {
	for i := int64(1); i < newCustomerCount+1; i++ {
		execVtgateQuery(t, vtgateConn, "customer@primary", fmt.Sprintf("insert into customer(name) values ('name-%d')", currentCustomerCount+i))
	}
	customerCount = getCustomerCount(t, "")
	require.Equal(t, currentCustomerCount+newCustomerCount, customerCount)
	currentCustomerCount = customerCount

	confirmLastCustomerIdHasIncreased(t)
}

func confirmGlobalRoutingToSource(t *testing.T) {
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
