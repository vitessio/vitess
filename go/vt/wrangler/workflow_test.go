/*
Copyright 2020 The Vitess Authors.

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

package wrangler

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var noResult = &sqltypes.Result{}

func getMoveTablesWorkflow(t *testing.T, cells, tabletTypes string) *VReplicationWorkflow {
	p := &VReplicationWorkflowParams{
		Workflow:                        "wf1",
		SourceKeyspace:                  "sourceks",
		TargetKeyspace:                  "targetks",
		Tables:                          "customer,corder",
		Cells:                           cells,
		TabletTypes:                     tabletTypes,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
		OnDDL:                           binlogdatapb.OnDDLAction_EXEC.String(),
	}
	mtwf := &VReplicationWorkflow{
		workflowType: MoveTablesWorkflow,
		ctx:          context.Background(),
		wr:           nil,
		params:       p,
		ts:           nil,
		ws:           nil,
	}
	return mtwf
}

func testComplete(t *testing.T, vrwf *VReplicationWorkflow) error {
	_, err := vrwf.Complete()
	return err
}
func TestReshardingWorkflowErrorsAndMisc(t *testing.T) {
	mtwf := getMoveTablesWorkflow(t, "cell1,cell2", "replica,rdonly")
	require.False(t, mtwf.Exists())
	mtwf.ws = &workflow.State{}
	require.True(t, mtwf.Exists())
	require.Errorf(t, testComplete(t, mtwf), ErrWorkflowNotFullySwitched)
	mtwf.ws.WritesSwitched = true
	require.Errorf(t, mtwf.Cancel(), ErrWorkflowPartiallySwitched)

	tabletTypes, _, err := discovery.ParseTabletTypesAndOrder(mtwf.params.TabletTypes)
	require.NoError(t, err)

	require.ElementsMatch(t, mtwf.getCellsAsArray(), []string{"cell1", "cell2"})
	require.ElementsMatch(t, tabletTypes, []topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY})
	hasReplica, hasRdonly, hasPrimary, err := mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.False(t, hasPrimary)

	mtwf.params.TabletTypes = "replica,rdonly,primary"
	tabletTypes, _, err = discovery.ParseTabletTypesAndOrder(mtwf.params.TabletTypes)
	require.NoError(t, err)
	require.ElementsMatch(t, tabletTypes,
		[]topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY, topodata.TabletType_PRIMARY})

	hasReplica, hasRdonly, hasPrimary, err = mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.True(t, hasPrimary)
}

func expectCanSwitchQueries(t *testing.T, tme *testMigraterEnv, keyspace, state string, currentLag int64) {
	now := time.Now().Unix()
	rowTemplate := "1|||||%s|vt_%s|%d|%d|0|0|||"
	row := fmt.Sprintf(rowTemplate, state, keyspace, now, now-currentLag)
	replicationResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|pos|stop_pos|max_replication_lag|state|db_name|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|message|tags",
		"int64|varchar|int64|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar"),
		row)
	copyStateResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table|lastpk",
		"varchar|varchar"),
		"t1|pk1",
	)

	for _, db := range tme.dbTargetClients {
		db.addInvariant(streamExtInfoKs2, replicationResult)

		if state == "Copying" {
			db.addInvariant(fmt.Sprintf(copyStateQuery, 1, 1), copyStateResult)
		} else {
			db.addInvariant(fmt.Sprintf(copyStateQuery, 1, 1), noResult)
		}
	}
}

// TestCanSwitch validates the logic to determine if traffic can be switched or not
func TestCanSwitch(t *testing.T) {
	var wf *VReplicationWorkflow
	ctx := context.Background()
	workflowName := "test"
	p := &VReplicationWorkflowParams{
		Workflow:       workflowName,
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,primary",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	expectCopyProgressQueries(t, tme)

	type testCase struct {
		name                  string
		state                 string
		streamLag, allowedLag int64 /* seconds */
		expectedReason        *regexp.Regexp
	}

	testCases := []testCase{
		{"In Copy Phase", binlogdatapb.VReplicationWorkflowState_Copying.String(), 0, 0, regexp.MustCompile(cannotSwitchCopyIncomplete)},
		{"High Lag", binlogdatapb.VReplicationWorkflowState_Running.String(), 6, 5, regexp.MustCompile(strings.ReplaceAll(cannotSwitchHighLag, "%d", "(\\d+)"))},
		{"Acceptable Lag", binlogdatapb.VReplicationWorkflowState_Running.String(), 4, 5, nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectCanSwitchQueries(t, tme, "ks2", tc.state, tc.streamLag)
			p.MaxAllowedTransactionLagSeconds = tc.allowedLag
			reason, err := wf.canSwitch("ks2", workflowName)
			require.NoError(t, err)

			if tc.expectedReason != nil {
				require.Regexp(t, tc.expectedReason, reason)

				m := tc.expectedReason.FindStringSubmatch(reason)
				switch tc.expectedReason.NumSubexp() {
				case 0:
					// cannotSwitchCopyIncomplete, nothing else to do
				case 2:
					// cannotSwitchHighLag, assert streamLag > allowedLag
					curLag, err := strconv.ParseInt(m[1], 10, 64)
					require.NoError(t, err, "could not parse current lag %s as int", m[1])

					allowedLag, err := strconv.ParseInt(m[2], 10, 64)
					require.NoError(t, err, "could not parse allowed lag %s as int", m[2])

					require.Greater(t, curLag, allowedLag, "current lag %d should be strictly greater than allowed lag %d (from reason %q)", curLag, allowedLag, reason)
				default:
					// unexpected regexp, fail loudly
					require.Fail(t, "unknown reason regexp %s -- did you add a new test case?", tc.expectedReason)
				}
			} else {
				require.Empty(t, reason, "should be able to switch, but cannot because %s", reason)
			}
		})
	}
}

func TestCopyProgress(t *testing.T) {
	var err error
	var wf *VReplicationWorkflow
	ctx := context.Background()
	workflowName := "test"
	p := &VReplicationWorkflowParams{
		Workflow:       workflowName,
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,primary",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err = tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())

	expectCopyProgressQueries(t, tme)

	var cp *CopyProgress
	cp, err = wf.GetCopyProgress()
	require.NoError(t, err)
	log.Infof("CopyProgress is %+v,%+v", (*cp)["t1"], (*cp)["t2"])

	require.Equal(t, int64(800), (*cp)["t1"].SourceRowCount)
	require.Equal(t, int64(200), (*cp)["t1"].TargetRowCount)
	require.Equal(t, int64(4000), (*cp)["t1"].SourceTableSize)
	require.Equal(t, int64(2000), (*cp)["t1"].TargetTableSize)

	require.Equal(t, int64(2000), (*cp)["t2"].SourceRowCount)
	require.Equal(t, int64(400), (*cp)["t2"].TargetRowCount)
	require.Equal(t, int64(4000), (*cp)["t2"].SourceTableSize)
	require.Equal(t, int64(1000), (*cp)["t2"].TargetTableSize)
}

func expectCopyProgressQueries(t *testing.T, tme *testMigraterEnv) {
	db := tme.tmeDB
	query := "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	rows := []string{"t1", "t2"}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table_name",
		"varchar"),
		rows...)
	db.AddQuery(query, result)
	query = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2"
	db.AddQuery(query, result)

	query = "select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_ks2' and table_name in ('t1','t2')"
	result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table_name|table_rows|data_length",
		"varchar|int64|int64"),
		"t1|100|1000",
		"t2|200|500")
	db.AddQuery(query, result)

	query = "select table_name, table_rows, data_length from information_schema.tables where table_schema = 'vt_ks1' and table_name in ('t1','t2')"
	result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table_name|table_rows|data_length",
		"varchar|int64|int64"),
		"t1|400|2000",
		"t2|1000|2000")
	db.AddQuery(query, result)

	for _, id := range []int{1, 2} {
		query = fmt.Sprintf("select distinct 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d", id)
		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"dummy",
			"int64"),
			"1")
		db.AddQuery(query, result)
	}
}

const defaultMaxAllowedTransactionLagSeconds = 30

func TestMoveTablesV2(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks1",
		TargetKeyspace:                  "ks2",
		Tables:                          "t1,t2",
		Cells:                           "cell1,cell2",
		TabletTypes:                     "REPLICA,RDONLY,PRIMARY",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
		OnDDL:                           binlogdatapb.OnDDLAction_STOP.String(),
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme, p)
	tme.expectNoPreviousJournals()
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, WorkflowStateAllSwitched, wf.CurrentState())

	tme.expectNoPreviousJournals()
	tme.expectNoPreviousReverseJournals()
	require.NoError(t, testReverse(t, wf))
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
}

// TestPartialMoveTables ensures that shard by shard migrations work
// as expected. This test moves tables from one sharded keyspace (ks1)
// to another sharded keyspace (ks2), but only for the -80 shard.
func TestPartialMoveTables(t *testing.T) {
	ctx := context.Background()
	shards := []string{"-80", "80-"}
	shardsToMove := shards[0:1]
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		WorkflowType:                    MoveTablesWorkflow,
		SourceKeyspace:                  "ks1",
		SourceShards:                    shardsToMove, // shard by shard
		TargetShards:                    shardsToMove, // shard by shard
		TargetKeyspace:                  "ks2",
		Tables:                          "t1,t2",
		Cells:                           "cell1,cell2",
		TabletTypes:                     "REPLICA,RDONLY,PRIMARY",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
		OnDDL:                           binlogdatapb.OnDDLAction_STOP.String(),
	}
	tme := newTestTablePartialMigrater(ctx, t, shards, shards[0:1], "select * %s")
	defer tme.stopTablets(t)

	// Save some unrelated shard routing rules to be sure that
	// they don't interfere in any way.
	srr, err := tme.ts.GetShardRoutingRules(ctx)
	require.NoError(t, err)
	srr.Rules = append(srr.Rules, []*vschema.ShardRoutingRule{
		{
			FromKeyspace: "wut",
			Shard:        "40-80",
			ToKeyspace:   "bloop",
		},
		{
			FromKeyspace: "haylo",
			Shard:        "-80",
			ToKeyspace:   "blarg",
		},
	}...)
	err = tme.ts.SaveShardRoutingRules(ctx, srr)
	require.NoError(t, err)

	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	require.True(t, wf.ts.isPartialMigration, "expected partial shard migration")

	// The default shard routing rule for the keyspace's other shard would
	// normally be put in place, but the unit test does not execute the
	// wrangler.MoveTables function which adds all of the default shard
	// routing rules in the topo for the keyspace when the first workflow
	// is run against it. So we simulate it here.
	srr, err = tme.ts.GetShardRoutingRules(ctx)
	require.NoError(t, err)
	srr.Rules = append(srr.Rules, &vschema.ShardRoutingRule{
		FromKeyspace: "ks2",
		Shard:        "80-",
		ToKeyspace:   "ks1",
	})
	err = tme.ts.SaveShardRoutingRules(ctx, srr)
	require.NoError(t, err)

	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme, p)
	tme.expectNoPreviousJournals()
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched, for shards: -80. Writes partially switched, for shards: -80", wf.CurrentState())
	require.NoError(t, err)

	tme.expectNoPreviousJournals()
	tme.expectNoPreviousReverseJournals()
	require.NoError(t, testReverse(t, wf))
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
}

func validateRoutingRuleCount(ctx context.Context, t *testing.T, ts *topo.Server, cnt int) {
	rr, err := ts.GetRoutingRules(ctx)
	require.NoError(t, err)
	require.NotNil(t, rr)
	rules := rr.Rules
	require.Equal(t, cnt, len(rules))
}

func checkIfTableExistInVSchema(ctx context.Context, t *testing.T, ts *topo.Server, keyspace string, table string) bool {
	vschema, err := ts.GetVSchema(ctx, keyspace)
	require.NoError(t, err)
	require.NotNil(t, vschema)
	_, ok := vschema.Tables[table]
	return ok
}

func TestMoveTablesV2Complete(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks1",
		TargetKeyspace:                  "ks2",
		Tables:                          "t1,t2",
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme, p)
	tme.expectNoPreviousJournals()
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, WorkflowStateAllSwitched, wf.CurrentState())

	//16 rules, 8 per table t1,t2 eg: t1,t1@replica,t1@rdonly,ks1.t1,ks1.t1@replica,ks1.t1@rdonly,ks2.t1@replica,ks2.t1@rdonly
	validateRoutingRuleCount(ctx, t, wf.wr.ts, 16)
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))
	require.NoError(t, testComplete(t, wf))
	require.False(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.False(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))

	validateRoutingRuleCount(ctx, t, wf.wr.ts, 0)
}

func testSwitchForward(t *testing.T, wf *VReplicationWorkflow) error {
	_, err := wf.SwitchTraffic(workflow.DirectionForward)
	return err
}

func testReverse(t *testing.T, wf *VReplicationWorkflow) error {
	_, err := wf.ReverseTraffic()
	return err
}

func TestMoveTablesV2SwitchTraffic(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks1",
		TargetKeyspace:                  "ks2",
		Tables:                          "t1,t2",
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme, p)

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "RDONLY"
	wf.params.Cells = "cell1"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched. Replica not switched. Rdonly switched in cells: cell1. Writes Not Switched", wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "rdonly"
	wf.params.Cells = "cell2"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched. Replica not switched. All Rdonly Reads Switched. Writes Not Switched", wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "REPLICA"
	wf.params.Cells = "cell1,cell2"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, WorkflowStateReadsSwitched, wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "replica,rdonly"
	require.NoError(t, testReverse(t, wf))
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "replica"
	wf.params.Cells = "cell1"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched. Replica switched in cells: cell1. Rdonly switched in cells: cell1. Writes Not Switched", wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "replica"
	wf.params.Cells = "cell2"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "All Reads Switched. Writes Not Switched", wf.CurrentState())
}

func TestMoveTablesV2Cancel(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks1",
		TargetKeyspace:                  "ks2",
		Tables:                          "t1,t2",
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	expectMoveTablesQueries(t, tme, p)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	expectMoveTablesQueries(t, tme, p)
	validateRoutingRuleCount(ctx, t, wf.wr.ts, 4) // rules set up by test env

	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))

	require.NoError(t, wf.Cancel())

	validateRoutingRuleCount(ctx, t, wf.wr.ts, 0)

	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))

	// Should target vschema table entries be deleted upon Cancel. For unsharded
	// keyspaces they should be as they are empty table entries that we also
	// create when the workflow is Created.
	targetVSchemaEntriesRemain := false
	if len(tme.targetShards) > 1 {
		// If the target keyspace is sharded -- which it is today in the test -- the
		// vschema must be created by the user before the workflow is started. Thus
		// we should also not delete the vschema table entries upon Cancel as the
		// management of the sharded vschema is up to the user.
		targetVSchemaEntriesRemain = true
	}
	require.Equal(t, targetVSchemaEntriesRemain, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.Equal(t, targetVSchemaEntriesRemain, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))
}

func TestReshardV2(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks",
		TargetKeyspace:                  "ks",
		SourceShards:                    sourceShards,
		TargetShards:                    targetShards,
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
		OnDDL:                           binlogdatapb.OnDDLAction_EXEC_IGNORE.String(),
	}
	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, ReshardWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectReshardQueries(t, tme, p)
	tme.expectNoPreviousJournals()
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, WorkflowStateAllSwitched, wf.CurrentState())
	require.NoError(t, testComplete(t, wf))
	si, err := wf.wr.ts.GetShard(ctx, "ks", "-40")
	require.Contains(t, err.Error(), "node doesn't exist")
	require.Nil(t, si)
	si, err = wf.wr.ts.GetShard(ctx, "ks", "-80")
	require.NoError(t, err)
	require.NotNil(t, si)
}

func TestVRWSchemaValidation(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-80", "80-"}
	targetShards := []string{"-40", "40-80", "80-c0", "c0-"}
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks",
		TargetKeyspace:                  "ks",
		SourceShards:                    sourceShards,
		TargetShards:                    targetShards,
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
	}
	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "not_in_vschema",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	for _, primary := range tme.sourcePrimaries {
		primary.FakeMysqlDaemon.Schema = schm
	}

	defer tme.stopTablets(t)
	vrwf, err := tme.wr.NewVReplicationWorkflow(ctx, ReshardWorkflow, p)
	vrwf.ws = nil
	require.NoError(t, err)
	require.NotNil(t, vrwf)
	shouldErr := vrwf.Create(ctx)
	require.Contains(t, shouldErr.Error(), "Create ReshardWorkflow failed: ValidateVSchema")
}

func TestReshardV2Cancel(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}
	p := &VReplicationWorkflowParams{
		Workflow:                        "test",
		SourceKeyspace:                  "ks",
		TargetKeyspace:                  "ks",
		SourceShards:                    sourceShards,
		TargetShards:                    targetShards,
		Cells:                           "cell1,cell2",
		TabletTypes:                     "replica,rdonly,primary",
		Timeout:                         DefaultActionTimeout,
		MaxAllowedTransactionLagSeconds: defaultMaxAllowedTransactionLagSeconds,
	}
	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, ReshardWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectReshardQueries(t, tme, p)
	require.NoError(t, wf.Cancel())
}

func expectReshardQueries(t *testing.T, tme *testShardMigraterEnv, params *VReplicationWorkflowParams) {
	vdiffDeleteQuery := fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
						where vd.keyspace = '%s' and vd.workflow = '%s'`,
		params.TargetKeyspace, params.Workflow)
	vdiffDeleteReverseQuery := fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
						where vd.keyspace = '%s' and vd.workflow = '%s_reverse'`,
		params.SourceKeyspace, params.Workflow)

	sourceQueries := []string{
		"select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'",
		"select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'",
	}
	noResult := &sqltypes.Result{}
	for _, dbclient := range tme.dbSourceClients {
		for _, query := range sourceQueries {
			dbclient.addInvariant(query, noResult)
		}
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid1)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.post_copy_action where vrepl_id in (1)", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		dbclient.addInvariant("insert into _vt.resharding_journal", noResult)
		dbclient.addInvariant("alter table _vt.copy_state auto_increment = 1", noResult)
	}

	targetQueries := []string{
		"select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'",
	}

	for _, dbclient := range tme.dbTargetClients {
		for _, query := range targetQueries {
			dbclient.addInvariant(query, noResult)
		}
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", noResult)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", noResult)
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pos|state|message",
			"varchar|varchar|varchar"),
			"MariaDB/5-456-892|Running")
		dbclient.addInvariant("select pos, state, message from _vt.vreplication where id=2", state)
		dbclient.addInvariant("select pos, state, message from _vt.vreplication where id=1", state)
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid1)
		dbclient.addInvariant("update _vt.vreplication set message = 'FROZEN'", noResult)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.post_copy_action where vrepl_id in (1)", noResult)
	}
	tme.tmeDB.AddQuery("USE `vt_ks`", noResult)
	tme.tmeDB.AddQuery("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1", noResult)
	tme.tmeDB.AddQuery("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2", noResult)
	tme.tmeDB.AddQuery(vdiffDeleteQuery, noResult)
	tme.tmeDB.AddQuery(vdiffDeleteReverseQuery, noResult)
	tme.tmeDB.AddQuery("alter table _vt.copy_state auto_increment = 1", noResult)
	tme.tmeDB.AddQuery("optimize table _vt.copy_state", noResult)
}

func expectMoveTablesQueries(t *testing.T, tme *testMigraterEnv, params *VReplicationWorkflowParams) {
	vdiffDeleteQuery := fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
	inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
	where vd.keyspace = '%s' and vd.workflow = '%s'`,
		params.TargetKeyspace, params.Workflow)
	vdiffDeleteReverseQuery := fmt.Sprintf(`delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
	inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
	where vd.keyspace = '%s' and vd.workflow = '%s_reverse'`,
		params.SourceKeyspace, params.Workflow)

	var query string
	noResult := &sqltypes.Result{}
	for _, dbclient := range tme.dbTargetClients {
		query = "update _vt.vreplication set state = 'Running', message = '' where id in (1)"
		dbclient.addInvariant(query, noResult)
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks2' and workflow = 'test'", resultid1)
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		query = "update _vt.vreplication set message='Picked source tablet: cell:\"cell1\" uid:10 ' where id=1"
		dbclient.addInvariant(query, noResult)
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", noResult)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("update _vt.vreplication set message = 'FROZEN'", noResult)
		dbclient.addInvariant("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", noResult)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.post_copy_action where vrepl_id in (1)", noResult)
		dbclient.addInvariant("insert into _vt.resharding_journal", noResult)
		dbclient.addInvariant("select val from _vt.resharding_journal", noResult)
		dbclient.addInvariant("select id, source, message, cell, tablet_types from _vt.vreplication where workflow='test_reverse' and db_name='vt_ks1'",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|source|message|cell|tablet_types",
				"int64|varchar|varchar|varchar|varchar"),
				""),
		)
	}

	for _, dbclient := range tme.dbSourceClients {
		dbclient.addInvariant("select val from _vt.resharding_journal", noResult)
		dbclient.addInvariant("update _vt.vreplication set message = 'FROZEN'", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", noResult)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", noResult)
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid1)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.post_copy_action where vrepl_id in (1)", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		dbclient.addInvariant("insert into _vt.resharding_journal", noResult)
		dbclient.addInvariant(reverseStreamExtInfoKs1, noResult)
	}
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running",
	)
	tme.dbTargetClients[0].addInvariant("select pos, state, message from _vt.vreplication where id=1", state)
	tme.dbTargetClients[0].addInvariant("select pos, state, message from _vt.vreplication where id=2", state)
	tme.dbTargetClients[1].addInvariant("select pos, state, message from _vt.vreplication where id=1", state)
	tme.dbTargetClients[1].addInvariant("select pos, state, message from _vt.vreplication where id=2", state)

	state = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-893|Running",
	)
	tme.dbSourceClients[0].addInvariant("select pos, state, message from _vt.vreplication where id=1", state)
	tme.dbSourceClients[0].addInvariant("select pos, state, message from _vt.vreplication where id=2", state)
	tme.dbSourceClients[1].addInvariant("select pos, state, message from _vt.vreplication where id=1", state)
	tme.dbSourceClients[1].addInvariant("select pos, state, message from _vt.vreplication where id=2", state)
	tme.tmeDB.AddQuery("USE `vt_ks1`", noResult)
	tme.tmeDB.AddQuery("USE `vt_ks2`", noResult)
	tme.tmeDB.AddQuery("drop table `vt_ks1`.`t1`", noResult)
	tme.tmeDB.AddQuery("drop table `vt_ks1`.`t2`", noResult)
	tme.tmeDB.AddQuery("drop table `vt_ks2`.`t1`", noResult)
	tme.tmeDB.AddQuery("drop table `vt_ks2`.`t2`", noResult)
	tme.tmeDB.AddQuery("update _vt.vreplication set message='Picked source tablet: cell:\"cell1\" uid:10 ' where id=1", noResult)
	tme.tmeDB.AddQuery("lock tables `t1` read,`t2` read", &sqltypes.Result{})
	tme.tmeDB.AddQuery("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1", noResult)
	tme.tmeDB.AddQuery("select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2", noResult)
	tme.tmeDB.AddQuery(vdiffDeleteQuery, noResult)
	tme.tmeDB.AddQuery(vdiffDeleteReverseQuery, noResult)
	tme.tmeDB.AddQuery("alter table _vt.copy_state auto_increment = 1", noResult)
	tme.tmeDB.AddQuery("optimize table _vt.copy_state", noResult)
}
