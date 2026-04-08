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

package vreplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type failingDBClient struct {
	connectErr   error
	failOnQuery  map[string]error
	supportsCaps bool
}

type recordingDBClient struct {
	queries []string
}

func (f *failingDBClient) DBName() string  { return "db" }
func (f *failingDBClient) Connect() error  { return f.connectErr }
func (f *failingDBClient) Begin() error    { return nil }
func (f *failingDBClient) Commit() error   { return nil }
func (f *failingDBClient) Rollback() error { return nil }
func (f *failingDBClient) Close()          {}
func (f *failingDBClient) IsClosed() bool  { return false }
func (f *failingDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	for key, err := range f.failOnQuery {
		if strings.Contains(query, key) {
			return nil, err
		}
	}
	if strings.Contains(query, getSQLModeQuery) {
		return sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("sql_mode", "varchar"),
			"STRICT_TRANS_TABLES,NO_ZERO_DATE,ANSI_QUOTES",
		), nil
	}
	if strings.Contains(query, "from _vt.vreplication where id=") {
		return sqlModeWorkflowSettingsResult(binlogdatapb.VReplicationWorkflowType_MoveTables), nil
	}
	if strings.Contains(query, "from _vt.copy_state where vrepl_id=") {
		return sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("count(distinct table_name)", "int64"),
			"0",
		), nil
	}
	return &sqltypes.Result{}, nil
}

func (f *failingDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	qr, err := f.ExecuteFetch(query, maxrows)
	if err != nil {
		return nil, err
	}
	return []*sqltypes.Result{qr}, nil
}

func (f *failingDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return f.supportsCaps, nil
}

func (r *recordingDBClient) DBName() string  { return "db" }
func (r *recordingDBClient) Connect() error  { return nil }
func (r *recordingDBClient) Begin() error    { return nil }
func (r *recordingDBClient) Commit() error   { return nil }
func (r *recordingDBClient) Rollback() error { return nil }
func (r *recordingDBClient) Close()          {}
func (r *recordingDBClient) IsClosed() bool  { return false }
func (r *recordingDBClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	r.queries = append(r.queries, query)
	return &sqltypes.Result{}, nil
}

func (r *recordingDBClient) ExecuteFetchMulti(query string, maxrows int) ([]*sqltypes.Result, error) {
	r.queries = append(r.queries, query)
	return []*sqltypes.Result{{}}, nil
}

func (r *recordingDBClient) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return false, nil
}

func TestApplyWorkerCloseRollsBack(t *testing.T) {
	worker := &applyWorker{}
	assert.NotPanics(t, func() {
		worker.close()
	})
}

func TestApplyWorkerRollbackNoError(t *testing.T) {
	worker := &applyWorker{}
	assert.NotPanics(t, func() {
		worker.rollback()
	})
}

func TestApplyWorkerApplyEventRestoresVPlayer(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := t.Context()

	originalClient := vp.dbClient
	vp.query = nil
	vp.commit = nil

	altDB := binlogplayer.NewMockDBClient(t)
	altClient := newVDBClient(altDB, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)

	worker := &applyWorker{ctx: ctx, client: altClient}
	worker.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return &sqltypes.Result{}, nil
	}
	worker.commit = func() error {
		return nil
	}

	gtid := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: gtid}

	err := worker.applyEvent(ctx, event, false, vp)
	require.NoError(t, err)

	expectedPos, err := binlogplayer.DecodePosition(gtid)
	require.NoError(t, err)
	assert.Equal(t, expectedPos.String(), vp.pos.String())

	assert.Equal(t, originalClient, vp.dbClient)
	assert.Nil(t, vp.query)
	assert.Nil(t, vp.commit)
}

func TestApplyWorkerApplyEventNilClientFailsFast(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := t.Context()

	initial := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1"
	pos, err := binlogplayer.DecodePosition(initial)
	require.NoError(t, err)
	vp.pos = pos

	worker := &applyWorker{ctx: ctx}
	event := &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_GTID, Gtid: "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"}

	err = worker.applyEvent(ctx, event, false, vp)
	require.ErrorContains(t, err, "apply worker has no active client")
	assert.Equal(t, pos.String(), vp.pos.String())
}

func TestApplyWorkerStatsReturnsVReplicatorStats(t *testing.T) {
	vp, _ := testVPlayer(t)
	worker := &applyWorker{vr: vp.vr}

	assert.Equal(t, vp.vr.stats, worker.stats())
}

func TestNewApplyWorker(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	mockDB := binlogplayer.NewMockDBClient(t)
	mockDB.AddInvariant("set @@session.time_zone", &sqltypes.Result{})
	mockDB.AddInvariant("set names 'binary'", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_read_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.net_write_timeout", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.sql_mode", &sqltypes.Result{})
	mockDB.AddInvariant("set @@session.foreign_key_checks", &sqltypes.Result{})
	mockDB.AddInvariant("select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys, options from _vt.vreplication where id=1", sqlModeWorkflowSettingsResult(binlogdatapb.VReplicationWorkflowType_MoveTables))
	mockDB.AddInvariant("select count(distinct table_name) from _vt.copy_state where vrepl_id=1", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("count(distinct table_name)", "int64"),
		"0",
	))
	mockDB.AddInvariant("max_allowed_packet", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("max_allowed_packet", "int64"),
		"4194304",
	))

	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(mockDB, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return mockDB }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.NoError(t, err)
	require.NotNil(t, worker)

	worker.close()
}

func TestCreateWorkerConn_UsesSerialSQLModeContract(t *testing.T) {
	testCases := []struct {
		name         string
		workflowType binlogdatapb.VReplicationWorkflowType
		expectedMode string
	}{
		{
			name:         "non-online-ddl uses exact sql mode",
			workflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
			expectedMode: SQLMode,
		},
		{
			name:         "online-ddl uses exact strict sql mode",
			workflowType: binlogdatapb.VReplicationWorkflowType_OnlineDDL,
			expectedMode: StrictSQLMode,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stats := binlogplayer.NewStats()
			stats.VReplicationLagGauges.Stop()
			teardownStats := stats
			defer teardownStats.Stop()

			config := vttablet.InitVReplicationConfigDefaults()
			workerDB := binlogplayer.NewMockDBClient(t)
			workerDB.RemoveInvariants("select @@session.sql_mode", "set @@session.sql_mode", "set @@session.foreign_key_checks")
			workerDB.AddInvariant("set @@session.time_zone", &sqltypes.Result{})
			workerDB.AddInvariant("set names 'binary'", &sqltypes.Result{})
			workerDB.AddInvariant("set @@session.net_read_timeout", &sqltypes.Result{})
			workerDB.AddInvariant("set @@session.net_write_timeout", &sqltypes.Result{})
			workerDB.AddInvariant("set @@session.sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')", &sqltypes.Result{})
			workerDB.AddInvariant("set @@session.sql_mode = REPLACE(REPLACE(REPLACE(@@session.sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', ''), 'NO_BACKSLASH_ESCAPES', '')", &sqltypes.Result{})
			workerDB.ExpectRequest(getSQLModeQuery, sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("sql_mode", "varchar"),
				"STRICT_TRANS_TABLES,NO_ZERO_DATE,ANSI_QUOTES",
			), nil)
			workerDB.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, sqlModeWorkflowSettingsResult(tc.workflowType), nil)
			workerDB.ExpectRequest("select count(distinct table_name) from _vt.copy_state where vrepl_id=1", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("count(distinct table_name)", "int64"),
				"0",
			), nil)
			workerDB.ExpectRequest(fmt.Sprintf(setSQLModeQueryf, tc.expectedMode), &sqltypes.Result{}, nil)
			workerDB.ExpectRequest("set @@session.foreign_key_checks=0", &sqltypes.Result{}, nil)

			vr := &vreplicator{
				id:             1,
				stats:          stats,
				dbClient:       newVDBClient(workerDB, stats, config.RelayLogMaxItems),
				workflowConfig: config,
				vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return workerDB }},
			}

			conn, err := createWorkerConn(t.Context(), vr)
			require.NoError(t, err)
			require.NotNil(t, conn)
			workerDB.Wait()
			conn.Close()
		})
	}
}

func TestNewApplyWorkerConnectError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	connectErr := errors.New("connect failed")
	badClient := &failingDBClient{connectErr: connectErr}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, connectErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerSettingsError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	settingsErr := errors.New("settings failed")
	badClient := &failingDBClient{failOnQuery: map[string]error{"time_zone": settingsErr}}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, settingsErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerClearFKCheckError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	fkErr := errors.New("fk checks failed")
	badClient := &failingDBClient{failOnQuery: map[string]error{"set @@session.foreign_key_checks=0": fkErr}}
	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(badClient, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return badClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, fkErr)
	require.Nil(t, worker)
}

func TestNewApplyWorkerClearFKRestrictError(t *testing.T) {
	stats := binlogplayer.NewStats()
	stats.VReplicationLagGauges.Stop()
	t.Cleanup(stats.Stop)

	config := vttablet.InitVReplicationConfigDefaults()

	restrictErr := errors.New("fk restrict failed")
	workerClient := &failingDBClient{failOnQuery: map[string]error{"set @@session.restrict_fk_on_non_standard_key=0": restrictErr}}
	capClient := &failingDBClient{supportsCaps: true}

	vr := &vreplicator{
		id:             1,
		stats:          stats,
		dbClient:       newVDBClient(capClient, stats, config.RelayLogMaxItems),
		workflowConfig: config,
		vre:            &Engine{dbClientFactoryFiltered: func() binlogplayer.DBClient { return workerClient }},
	}

	worker, err := newApplyWorker(t.Context(), vr)
	require.ErrorIs(t, err, restrictErr)
	require.Nil(t, worker)
}

func TestApplyWorkerApplyEventSetsFKChecksAfterRotate(t *testing.T) {
	vp, _ := testVPlayer(t)
	ctx := t.Context()
	vp.tablePlans["t1"] = &TablePlan{TargetName: "t1"}
	vp.vr.state = binlogdatapb.VReplicationWorkflowState_Running

	db0 := &recordingDBClient{}
	db1 := &recordingDBClient{}
	worker := &applyWorker{
		ctx:    ctx,
		conns:  [2]*vdbClient{newVDBClient(db0, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems), newVDBClient(db1, vp.vr.stats, vp.vr.workflowConfig.RelayLogMaxItems)},
		active: 0,
	}
	worker.client = worker.conns[0]
	worker.bindFunctions()

	vp.query = worker.query
	vp.commit = worker.commit
	vp.dbClient = worker.client
	rowEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ROW,
		RowEvent: &binlogdatapb.RowEvent{
			Flags:     0,
			TableName: "t1",
		},
	}

	require.NoError(t, worker.applyEvent(ctx, rowEvent, false, vp))
	assert.Contains(t, db0.queries, "set @@session.foreign_key_checks=true")

	worker.rotate()
	vp.query = worker.query
	vp.commit = worker.commit
	vp.dbClient = worker.client

	require.NoError(t, worker.applyEvent(ctx, rowEvent, false, vp))
	assert.Contains(t, db1.queries, "set @@session.foreign_key_checks=true")
}

func sqlModeWorkflowSettingsResult(workflowType binlogdatapb.VReplicationWorkflowType) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pos", Type: sqltypes.VarBinary},
			{Name: "stop_pos", Type: sqltypes.VarBinary},
			{Name: "max_tps", Type: sqltypes.Int64},
			{Name: "max_replication_lag", Type: sqltypes.Int64},
			{Name: "state", Type: sqltypes.VarBinary},
			{Name: "workflow_type", Type: sqltypes.Int64},
			{Name: "workflow", Type: sqltypes.VarChar},
			{Name: "workflow_sub_type", Type: sqltypes.Int64},
			{Name: "defer_secondary_keys", Type: sqltypes.Int64},
			{Name: "options", Type: sqltypes.VarBinary},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarBinary("MariaDB/0-1-1083"),
			sqltypes.NULL,
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()),
			sqltypes.NewInt64(int64(workflowType)),
			sqltypes.NewVarChar("wf"),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewVarBinary("{}"),
		}},
	}
}
