/*
Copyright 2019 The Vitess Authors.

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

package binlogplayer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	testSettingsResponse = &sqltypes.Result{
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
		},
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"), // pos
				sqltypes.NULL,                          // stop_pos
				sqltypes.NewInt64(9223372036854775807), // max_tps
				sqltypes.NewInt64(9223372036854775807), // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),      // workflow_type
				sqltypes.NewVarChar("wf"), // workflow
				sqltypes.NewInt64(0),      // workflow_sub_type
				sqltypes.NewInt64(0),      // defer_secondary_keys
			},
		},
	}
	testDMLResponse = &sqltypes.Result{RowsAffected: 1}
	testPos         = "MariaDB/0-1-1083"
)

func TestNewBinlogPlayerKeyRange(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	fbc := newFakeBinlogClient()
	wantTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell",
			Uid:  1,
		},
		Keyspace: "ks",
		Shard:    "0",
	}
	wantKeyRange := &topodatapb.KeyRange{End: []byte{0x80}}

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerKeyRange(dbClient, wantTablet, wantKeyRange, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()
	expectFBCRequest(t, fbc, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})

	require.NoError(t, errfunc())

	record := logs.Record(t, "BinlogPlayer client for keyrange starting")
	assert.Equal(t, "wf", record["workflow"])
	assert.EqualValues(t, 1, record["stream_id"])
	assert.Equal(t, "-80", record["key_range"])
	assert.Equal(t, testPos, record["position"])
	assert.Equal(t, "cell-0000000001", record["tablet"])
}

func TestNewBinlogPlayerTables(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	fbc := newFakeBinlogClient()
	wantTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell",
			Uid:  1,
		},
		Keyspace: "ks",
		Shard:    "0",
	}
	wantTables := []string{"a", "b"}

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, wantTablet, wantTables, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()
	expectFBCRequest(t, fbc, wantTablet, testPos, wantTables, nil)

	require.NoError(t, errfunc())

	record := logs.Record(t, "BinlogPlayer client for tables starting")
	assert.Equal(t, "wf", record["workflow"])
	assert.EqualValues(t, 1, record["stream_id"])
	assert.Equal(t, []any{"a", "b"}, record["tables"])
	assert.Equal(t, testPos, record["position"])
	assert.Equal(t, "cell-0000000001", record["tablet"])
}

// TestApplyEventsFail ensures the error is recorded in the vreplication table if there's a failure.
func TestApplyEventsFail(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, errors.New("err"))
	dbClient.ExpectRequest("update _vt.vreplication set state='Error', message='error in processing binlog event failed query BEGIN, err: err' where id=1", testDMLResponse, nil)

	fbc := newFakeBinlogClient()
	fbc.txn = &binlogdatapb.BinlogTransaction{
		Statements: []*binlogdatapb.BinlogTransaction_Statement{
			{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into t values('row-value-1')")},
			{Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT, Sql: []byte("insert into t values('row-value-2')")},
		},
		EventToken: &querypb.EventToken{Position: "MariaDB/0-1-1235"},
	}

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()

	want := "error in processing binlog event failed query BEGIN, err: err"
	err := errfunc()
	require.EqualErrorf(t, err, want, "ApplyBinlogEvents err: %v, want %v", err, want)

	record := logs.Record(t, "transaction failed")
	assert.Equal(t, "wf", record["workflow"])
	assert.EqualValues(t, 1, record["stream_id"])
	assert.Equal(t, "failed query BEGIN, err: err", record["error"])
	var queries []any
	for _, record := range logs.Records(t) {
		assert.NotContains(t, record["msg"], "row-value")
		if record["msg"] == "statement" {
			assert.EqualValues(t, 1, record["stream_id"])
			queries = append(queries, record["query"])
		}
	}
	assert.Equal(t, []any{`"insert into t values('row-value-1')"`, `"insert into t values('row-value-2')"`}, queries)
}

// TestApplyEventsLogsSettingsErrors checks the records logged when the player
// can't write its state or read its settings, before the workflow name is
// known.
func TestApplyEventsLogsSettingsErrors(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", nil, errors.New("running state failed"))
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, nil, errors.New("settings failed"))
	dbClient.ExpectRequestRE("update _vt.vreplication set state='Error', message='.*settings failed.*' where id=1", nil, errors.New("stop state failed"))

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()
	require.ErrorContains(t, errfunc(), "settings failed")

	for msg, wantErr := range map[string]string{
		"Error writing Running state":             "running state failed",
		"Error reading the VReplication settings": "settings failed",
		"Error writing stop state":                "stop state failed",
	} {
		record := logs.Record(t, msg)
		assert.EqualValues(t, 1, record["stream_id"], msg)
		assert.NotContains(t, record, "workflow", msg)
		assert.Contains(t, record["error"], wantErr, msg)
		assert.NotContains(t, record, "error_code", msg)
	}
}

// TestApplyEventsLogsSourceErrors checks the records logged when the player
// can't dial the source tablet or start the stream.
func TestApplyEventsLogsSourceErrors(t *testing.T) {
	for _, tc := range []struct {
		msg, wantMessage   string
		dialErr, streamErr error
	}{{
		msg:         "error dialing binlog server",
		wantMessage: "error dialing binlog server: ",
		dialErr:     vterrors.New(vtrpcpb.Code_UNAVAILABLE, "source failed"),
	}, {
		msg:         "error sending streaming query to binlog server",
		wantMessage: "error sending streaming query to binlog server: ",
		streamErr:   vterrors.New(vtrpcpb.Code_UNAVAILABLE, "source failed"),
	}} {
		t.Run(tc.msg, func(t *testing.T) {
			logs := logtest.Capture(t, slog.LevelInfo)
			dbClient := NewMockDBClient(t)
			dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
			dbClient.ExpectRequest(TestGetWorkflowQueryId1, testSettingsResponse, nil)
			dbClient.ExpectRequestRE("(?s)update _vt.vreplication set state='Error', message='"+tc.wantMessage+".*source failed.*' where id=1", testDMLResponse, nil)

			fbc := newFakeBinlogClient()
			fbc.dialErr = tc.dialErr
			fbc.streamErr = tc.streamErr

			stats := NewStats()
			defer stats.Stop()
			blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
			errfunc := applyEvents(blp)

			dbClient.Wait()
			require.ErrorContains(t, errfunc(), "source failed")

			record := logs.Record(t, tc.msg)
			assert.Equal(t, "wf", record["workflow"])
			assert.EqualValues(t, 1, record["stream_id"])
			assert.Equal(t, "source failed", record["error"])
			assert.Equal(t, "UNAVAILABLE", record["error_code"])
		})
	}
}

var settingsFields []*querypb.Field = []*querypb.Field{
	{Name: "pos", Type: sqltypes.VarBinary},
	{Name: "stop_pos", Type: sqltypes.VarBinary},
	{Name: "max_tps", Type: sqltypes.Int64},
	{Name: "max_replication_lag", Type: sqltypes.Int64},
	{Name: "state", Type: sqltypes.VarBinary},
	{Name: "workflow_type", Type: sqltypes.Int64},
	{Name: "workflow", Type: sqltypes.VarChar},
	{Name: "workflow_sub_type", Type: sqltypes.Int64},
	{Name: "defer_secondary_keys", Type: sqltypes.Int64},
}

// TestStopPosEqual ensures player stops if stopPos==pos.
func TestStopPosEqual(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       settingsFields,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, posEqual, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='not starting BinlogPlayer, we\'re already at the desired position 0-1-1083' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()

	require.NoError(t, errfunc())

	record := logs.Record(t, "not starting BinlogPlayer, we're already at the desired position")
	assert.Equal(t, "wf", record["workflow"])
	assert.Equal(t, "MariaDB/0-1-1083", record["stop_position"])
}

// TestStopPosLess ensures player stops if stopPos<pos.
func TestStopPosLess(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       settingsFields,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1082"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, posEqual, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='starting point 0-1-1083 greater than stopping point 0-1-1082' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()

	require.NoError(t, errfunc())

	record := logs.Record(t, "starting point greater than stopping point")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "wf", record["workflow"])
	assert.Equal(t, "MariaDB/0-1-1083", record["start_position"])
	assert.Equal(t, "MariaDB/0-1-1082", record["stop_position"])
}

// TestStopPosGreater ensures player stops if stopPos>pos.
func TestStopPosGreater(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       settingsFields,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1085"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, posEqual, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()

	require.NoError(t, errfunc())

	record := logs.Record(t, "Will stop player when reaching the stop position")
	assert.Equal(t, "MariaDB/0-1-1085", record["stop_position"])
	record = logs.Record(t, "Reached stopping position, done playing logs")
	assert.Equal(t, "wf", record["workflow"])
	assert.EqualValues(t, 1, record["stream_id"])
}

// TestContextCancel ensures player does not record error or stop if context is canceled.
func TestContextCancel(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       settingsFields,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1085"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, posEqual, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	errfunc := applyEvents(blp)

	dbClient.Wait()

	// Wait for Apply to return,
	// and call dbClient.Wait to ensure
	// no new statements were issued.
	require.NoError(t, errfunc())

	dbClient.Wait()
}

func TestRetryOnDeadlock(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(TestGetWorkflowQueryId1, testSettingsResponse, nil)
	deadlocked := &sqlerror.SQLError{Num: 1213, Message: "deadlocked"}
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", nil, deadlocked)
	dbClient.ExpectRequest("rollback", nil, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	blp.deadlockRetry = 10 * time.Millisecond
	errfunc := applyEvents(blp)

	dbClient.Wait()

	require.NoError(t, errfunc())

	record := logs.Record(t, "Deadlock")
	assert.Equal(t, "wf", record["workflow"])
	assert.Contains(t, record["error"], "deadlocked")
	record = logs.Record(t, "Retrying txn")
	assert.EqualValues(t, 10*time.Millisecond, record["retry_delay"])
}

func TestSlowQueryLog(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	saved := SlowQueryThreshold
	SlowQueryThreshold = -1
	t.Cleanup(func() { SlowQueryThreshold = saved })

	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("insert into t values('row-value')", testDMLResponse, nil)
	stats := NewStats()
	defer stats.Stop()
	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, stats)
	_, err := blp.exec("insert into t values('row-value')")
	require.NoError(t, err)

	record := logs.Record(t, "SLOW QUERY")
	assert.EqualValues(t, 1, record["stream_id"])
	assert.Equal(t, "insert into t values('row-value')", record["query"])
	assert.Greater(t, record["duration"], 0.0)
}

// applyEvents starts a goroutine to apply events, and returns an error function.
// The error func must be invoked before exiting the test to ensure that apply
// has finished. Otherwise, it may cause race with other tests.
func applyEvents(blp *BinlogPlayer) func() error {
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		errChan <- blp.ApplyBinlogEvents(ctx)
	}()

	return func() error {
		cancel()
		return <-errChan
	}
}

func TestCreateVReplicationKeyRange(t *testing.T) {
	source := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		KeyRange: &topodatapb.KeyRange{
			End: []byte{0x80},
		},
	}
	want := fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options) "+
		"values (%v, %v, %v, %v, %v, %v, 0, %v, %v, %d, %d, %v, %s)", encodeString("Resharding"), encodeString(source.String()), encodeString("MariaDB/0-1-1083"), 9223372036854775807, 9223372036854775807, 481823, encodeString("Running"), encodeString("db"), 0, 0, false, encodeString("{}"))

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		KeyRange: &topodatapb.KeyRange{
			End: []byte{0x80},
		},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823, "db", 0, 0, false)
	assert.Equalf(t, want, got, "CreateVReplication() =\n%v, want\n%v", got, want)
}

func TestCreateVReplicationTables(t *testing.T) {
	source := &binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		Tables:   []string{"a", "b"},
	}
	want := fmt.Sprintf("insert into _vt.vreplication "+
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options) "+
		"values (%v, %v, %v, %v, %v, %v, 0, %v, %v, %d, %d, %v, %s)", encodeString("Resharding"), encodeString(source.String()), encodeString("MariaDB/0-1-1083"), 9223372036854775807, 9223372036854775807, 481823, encodeString("Running"), encodeString("db"), 0, 0, false, encodeString("{}"))

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		Tables:   []string{"a", "b"},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823, "db", 0, 0, false)
	assert.Equalf(t, want, got, "CreateVReplication() =\n%v, want\n%v", got, want)
}

func TestUpdateVReplicationPos(t *testing.T) {
	gtid := replication.MustParseGTID("MariaDB", "0-1-8283")
	want := "update _vt.vreplication " +
		"set pos='MariaDB/0-1-8283', time_updated=88822, rows_copied=0, message='' " +
		"where id=78522"

	got := GenerateUpdatePos(78522, replication.Position{GTIDSet: gtid.GTIDSet()}, 88822, 0, 0, false)
	assert.Equalf(t, want, got, "updateVReplicationPos() = %#v, want %#v", got, want)
}

func TestUpdateVReplicationTimestamp(t *testing.T) {
	gtid := replication.MustParseGTID("MariaDB", "0-2-582")
	want := "update _vt.vreplication " +
		"set pos='MariaDB/0-2-582', time_updated=88822, transaction_timestamp=481828, rows_copied=0, message='' " +
		"where id=78522"

	got := GenerateUpdatePos(78522, replication.Position{GTIDSet: gtid.GTIDSet()}, 88822, 481828, 0, false)
	assert.Equalf(t, want, got, "updateVReplicationPos() = %#v, want %#v", got, want)
}

func TestReadVReplicationPos(t *testing.T) {
	want := "select pos from _vt.vreplication where id=482821"
	got := ReadVReplicationPos(482821)
	assert.Equalf(t, want, got, "ReadVReplicationPos(482821) = %#v, want %#v", got, want)
}

func TestReadVReplicationStatus(t *testing.T) {
	want := "select pos, state, message from _vt.vreplication where id=482821"
	got := ReadVReplicationStatus(482821)
	assert.Equalf(t, want, got, "ReadVReplicationStatus(482821) = %#v, want %#v", got, want)
}

func TestEncodeString(t *testing.T) {
	tcases := []struct {
		in, out string
	}{
		{
			in:  "",
			out: "''",
		},
		{
			in:  "a",
			out: "'a'",
		},
		{
			in:  "here's",
			out: "'here\\'s'",
		},
		{
			in:  "online-ddl is denied access due to lag metric value 94.821447 exceeding threshold 5",
			out: "'online-ddl is denied access due to lag metric value 94.821447 exceeding threshold 5'",
		},
		{
			in:  "'a','b','c'",
			out: "'\\'a\\',\\'b\\',\\'c\\''",
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.in, func(t *testing.T) {
			out := encodeString(tcase.in)
			assert.Equal(t, tcase.out, out)
		})
	}
}

func TestMessageTruncate(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectTruncate bool
	}{
		{
			name:           "short message unchanged",
			input:          "short message",
			expectTruncate: false,
		},
		{
			name:           "exactly 950 bytes unchanged",
			input:          strings.Repeat("a", 950),
			expectTruncate: false,
		},
		{
			name:           "951 bytes truncated",
			input:          strings.Repeat("b", 951),
			expectTruncate: true,
		},
		{
			name:           "very long message truncated",
			input:          strings.Repeat("c", 2000),
			expectTruncate: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := logtest.Capture(t, slog.LevelInfo)
			result := MessageTruncate(tc.input)
			if tc.expectTruncate {
				assert.LessOrEqual(t, len(result), 950, "truncated message should be at most 950 bytes")
				assert.Contains(t, result, TruncationIndicator, "truncated message should contain truncation indicator")
				record := logs.Record(t, "VReplication message truncated")
				assert.Equal(t, "WARN", record["level"])
				assert.EqualValues(t, len(tc.input), record["length"])
				assert.Equal(t, tc.input, record["message"])
			} else {
				assert.Equal(t, tc.input, result, "message should be unchanged")
				assert.Empty(t, logs.Records(t))
			}
		})
	}
}
