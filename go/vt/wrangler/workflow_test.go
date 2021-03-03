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
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/topo"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func getMoveTablesWorkflow(t *testing.T, cells, tabletTypes string) *VReplicationWorkflow {
	p := &VReplicationWorkflowParams{
		Workflow:       "wf1",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		Tables:         "customer,corder",
		Cells:          cells,
		TabletTypes:    tabletTypes,
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
	mtwf.ws = &workflowState{}
	require.True(t, mtwf.Exists())
	require.Errorf(t, testComplete(t, mtwf), ErrWorkflowNotFullySwitched)
	mtwf.ws.WritesSwitched = true
	require.Errorf(t, mtwf.Cancel(), ErrWorkflowPartiallySwitched)

	require.ElementsMatch(t, mtwf.getCellsAsArray(), []string{"cell1", "cell2"})
	require.ElementsMatch(t, mtwf.getTabletTypes(), []topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY})
	hasReplica, hasRdonly, hasMaster, err := mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.False(t, hasMaster)

	mtwf.params.TabletTypes = "replica,rdonly,master"
	require.ElementsMatch(t, mtwf.getTabletTypes(),
		[]topodata.TabletType{topodata.TabletType_REPLICA, topodata.TabletType_RDONLY, topodata.TabletType_MASTER})

	hasReplica, hasRdonly, hasMaster, err = mtwf.parseTabletTypes()
	require.NoError(t, err)
	require.True(t, hasReplica)
	require.True(t, hasRdonly)
	require.True(t, hasMaster)
}

func TestCopyProgress(t *testing.T) {
	var err error
	var wf *VReplicationWorkflow
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:       "test",
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
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

	var isCopyInProgress bool
	isCopyInProgress, err = wf.IsCopyInProgress()
	require.NoError(t, err)
	require.True(t, isCopyInProgress)
}

func expectCopyProgressQueries(t *testing.T, tme *testMigraterEnv) {
	db := tme.tmeDB
	query := "select table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	rows := []string{"t1", "t2"}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table_name",
		"varchar"),
		rows...)
	db.AddQuery(query, result)
	query = "select table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2"
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
		query = fmt.Sprintf("select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d", id)
		result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"dummy",
			"int64"),
			"1")
		db.AddQuery(query, result)
	}
}

func TestMoveTablesV2(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:       "test",
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme)
	tme.expectNoPreviousJournals()
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, WorkflowStateAllSwitched, wf.CurrentState())

	tme.expectNoPreviousJournals()
	tme.expectNoPreviousReverseJournals()
	require.NoError(t, testReverse(t, wf))
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
}

func validateRoutingRuleCount(ctx context.Context, t *testing.T, ts *topo.Server, cnt int) {
	rr, err := ts.GetRoutingRules(ctx)
	fmt.Printf("Rules %+v\n", rr.Rules)
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
		Workflow:       "test",
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme)
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
	_, err := wf.SwitchTraffic(DirectionForward)
	return err
}

func testReverse(t *testing.T, wf *VReplicationWorkflow) error {
	_, err := wf.ReverseTraffic()
	return err
}

func TestMoveTablesV2Partial(t *testing.T) {
	ctx := context.Background()
	p := &VReplicationWorkflowParams{
		Workflow:       "test",
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectMoveTablesQueries(t, tme)

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "rdonly"
	wf.params.Cells = "cell1"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched. Replica not switched. Rdonly switched in cells: cell1. Writes Not Switched", wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "rdonly"
	wf.params.Cells = "cell2"
	require.NoError(t, testSwitchForward(t, wf))
	require.Equal(t, "Reads partially switched. Replica not switched. All Rdonly Reads Switched. Writes Not Switched", wf.CurrentState())

	tme.expectNoPreviousJournals()
	wf.params.TabletTypes = "replica"
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
		Workflow:       "test",
		SourceKeyspace: "ks1",
		TargetKeyspace: "ks2",
		Tables:         "t1,t2",
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)
	expectMoveTablesQueries(t, tme)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, MoveTablesWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	expectMoveTablesQueries(t, tme)
	validateRoutingRuleCount(ctx, t, wf.wr.ts, 4) // rules set up by test env

	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))

	require.NoError(t, wf.Cancel())

	validateRoutingRuleCount(ctx, t, wf.wr.ts, 0)

	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t1"))
	require.True(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks1", "t2"))
	require.False(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t1"))
	require.False(t, checkIfTableExistInVSchema(ctx, t, wf.wr.ts, "ks2", "t2"))
}

func TestReshardV2(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}
	p := &VReplicationWorkflowParams{
		Workflow:       "test",
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
		SourceShards:   sourceShards,
		TargetShards:   targetShards,
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, ReshardWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectReshardQueries(t, tme)
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

func TestReshardV2Cancel(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-40", "40-"}
	targetShards := []string{"-80", "80-"}
	p := &VReplicationWorkflowParams{
		Workflow:       "test",
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
		SourceShards:   sourceShards,
		TargetShards:   targetShards,
		Cells:          "cell1,cell2",
		TabletTypes:    "replica,rdonly,master",
		Timeout:        DefaultActionTimeout,
	}
	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	defer tme.stopTablets(t)
	wf, err := tme.wr.NewVReplicationWorkflow(ctx, ReshardWorkflow, p)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Equal(t, WorkflowStateNotSwitched, wf.CurrentState())
	tme.expectNoPreviousJournals()
	expectReshardQueries(t, tme)
	require.NoError(t, wf.Cancel())
}

func expectReshardQueries(t *testing.T, tme *testShardMigraterEnv) {

	sourceQueries := []string{
		"select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'",
		"select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'",
	}
	noResult := &sqltypes.Result{}
	for _, dbclient := range tme.dbSourceClients {
		for _, query := range sourceQueries {
			dbclient.addInvariant(query, noResult)
		}
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid1)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		dbclient.addInvariant("insert into _vt.resharding_journal", noResult)
	}

	targetQueries := []string{
		"select id, workflow, source, pos from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'",
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
	}
	tme.tmeDB.AddQuery("select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1", noResult)
	tme.tmeDB.AddQuery("select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2", noResult)

}

func expectMoveTablesQueries(t *testing.T, tme *testMigraterEnv) {
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
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("update _vt.vreplication set message = 'FROZEN'", noResult)
		dbclient.addInvariant("select 1 from _vt.vreplication where db_name='vt_ks2' and workflow='test' and message!='FROZEN'", noResult)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
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
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (1)", noResult)
		dbclient.addInvariant("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id in (2)", noResult)
		dbclient.addInvariant("select id from _vt.vreplication where id = 1", resultid1)
		dbclient.addInvariant("select id from _vt.vreplication where id = 2", resultid2)
		dbclient.addInvariant("select id from _vt.vreplication where db_name = 'vt_ks1' and workflow = 'test_reverse'", resultid1)
		dbclient.addInvariant("delete from _vt.vreplication where id in (1)", noResult)
		dbclient.addInvariant("delete from _vt.copy_state where vrepl_id in (1)", noResult)
		dbclient.addInvariant("insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name)", &sqltypes.Result{InsertID: uint64(1)})
		dbclient.addInvariant("select * from _vt.vreplication where id = 1", runningResult(1))
		dbclient.addInvariant("select * from _vt.vreplication where id = 2", runningResult(2))
		dbclient.addInvariant("insert into _vt.resharding_journal", noResult)
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
	tme.tmeDB.AddQuery("drop table vt_ks1.t1", noResult)
	tme.tmeDB.AddQuery("drop table vt_ks1.t2", noResult)
	tme.tmeDB.AddQuery("drop table vt_ks2.t1", noResult)
	tme.tmeDB.AddQuery("drop table vt_ks2.t2", noResult)
	tme.tmeDB.AddQuery("update _vt.vreplication set message='Picked source tablet: cell:\"cell1\" uid:10 ' where id=1", noResult)
	tme.tmeDB.AddQuery("select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1", noResult)
	tme.tmeDB.AddQuery("select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 2", noResult)

}
