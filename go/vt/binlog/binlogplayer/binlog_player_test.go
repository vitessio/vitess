/*
Copyright 2017 Google Inc.

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
	"errors"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/throttler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	testSettingsResponse = &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"), // pos
				sqltypes.NULL, // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
				sqltypes.NewVarBinary("Running"),             // state
			},
		},
	}
	testDMLResponse = &sqltypes.Result{RowsAffected: 1}
	testPos         = "MariaDB/0-1-1083"
)

func TestNewBinlogPlayerKeyRange(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
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

	blp := NewBinlogPlayerKeyRange(dbClient, wantTablet, wantKeyRange, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()
	expectFBCRequest(t, fbc, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})

	if err := errfunc(); err != nil {
		t.Error(err)
	}
}

func TestNewBinlogPlayerTables(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
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

	blp := NewBinlogPlayerTables(dbClient, wantTablet, wantTables, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()
	expectFBCRequest(t, fbc, wantTablet, testPos, wantTables, nil)

	if err := errfunc(); err != nil {
		t.Error(err)
	}
}

// TestApplyEventsFail ensures the error is recorded in the vreplication table if there's a failure.
func TestApplyEventsFail(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, errors.New("err"))
	dbClient.ExpectRequest("update _vt.vreplication set state='Error', message='error in processing binlog event failed query BEGIN, err: err' where id=1", testDMLResponse, nil)

	_ = newFakeBinlogClient()

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()

	want := "error in processing binlog event failed query BEGIN, err: err"
	if err := errfunc(); err == nil || err.Error() != want {
		t.Errorf("ApplyBinlogEvents err: %v, want %v", err, want)
	}
}

// TestStopPosEqual ensures player stops if stopPos==pos.
func TestStopPosEqual(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
				sqltypes.NewVarBinary("Running"),             // state
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", posEqual, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='not starting BinlogPlayer, we\'re already at the desired position 0-1-1083' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()

	if err := errfunc(); err != nil {
		t.Error(err)
	}
}

// TestStopPosLess ensures player stops if stopPos<pos.
func TestStopPosLess(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1082"),    // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
				sqltypes.NewVarBinary("Running"),             // state
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", posEqual, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='starting point 0-1-1083 greater than stopping point 0-1-1082' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()

	if err := errfunc(); err != nil {
		t.Error(err)
	}
}

// TestStopPosGreater ensures player stops if stopPos>pos.
func TestStopPosGreater(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1085"),    // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
				sqltypes.NewVarBinary("Running"),             // state
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", posEqual, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()

	if err := errfunc(); err != nil {
		t.Error(err)
	}
}

// TestContextCancel ensures player does not record error or stop if context is canceled.
func TestContextCancel(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	posEqual := &sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),    // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1085"),    // stop_pos
				sqltypes.NewVarBinary("9223372036854775807"), // max_tps
				sqltypes.NewVarBinary("9223372036854775807"), // max_replication_lag
				sqltypes.NewVarBinary("Running"),             // state
			},
		},
	}
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", posEqual, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest(`update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1`, testDMLResponse, nil)

	_ = newFakeBinlogClient()

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	errfunc := applyEvents(blp)

	dbClient.Wait()

	// Wait for Apply to return,
	// and call dbClient.Wait to ensure
	// no new statements were issued.
	if err := errfunc(); err != nil {
		t.Error(err)
	}

	dbClient.Wait()
}

func TestRetryOnDeadlock(t *testing.T) {
	dbClient := NewMockDBClient(t)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest("select pos, stop_pos, max_tps, max_replication_lag, state from _vt.vreplication where id=1", testSettingsResponse, nil)
	deadlocked := &mysql.SQLError{Num: 1213, Message: "deadlocked"}
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", nil, deadlocked)
	dbClient.ExpectRequest("rollback", nil, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	blp := NewBinlogPlayerTables(dbClient, nil, []string{"a"}, 1, NewStats())
	blp.deadlockRetry = 10 * time.Millisecond
	errfunc := applyEvents(blp)

	dbClient.Wait()

	if err := errfunc(); err != nil {
		t.Error(err)
	}
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
	want := "insert into _vt.vreplication " +
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) " +
		`values ('Resharding', 'keyspace:\"ks\" shard:\"0\" key_range:<end:\"\\200\" > ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		KeyRange: &topodatapb.KeyRange{
			End: []byte{0x80},
		},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823)
	if got != want {
		t.Errorf("CreateVReplication() =\n%v, want\n%v", got, want)
	}
}

func TestCreateVReplicationTables(t *testing.T) {
	want := "insert into _vt.vreplication " +
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) " +
		`values ('Resharding', 'keyspace:\"ks\" shard:\"0\" tables:\"a\" tables:\"b\" ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		Tables:   []string{"a", "b"},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823)
	if got != want {
		t.Errorf("CreateVReplication() =\n%v, want\n%v", got, want)
	}
}

func TestUpdateVReplicationPos(t *testing.T) {
	gtid := mysql.MustParseGTID("MariaDB", "0-1-8283")
	want := "update _vt.vreplication " +
		"set pos='MariaDB/0-1-8283', time_updated=88822 " +
		"where id=78522"

	got := GenerateUpdatePos(78522, mysql.Position{GTIDSet: gtid.GTIDSet()}, 88822, 0)
	if got != want {
		t.Errorf("updateVReplicationPos() = %#v, want %#v", got, want)
	}
}

func TestUpdateVReplicationTimestamp(t *testing.T) {
	gtid := mysql.MustParseGTID("MariaDB", "0-2-582")
	want := "update _vt.vreplication " +
		"set pos='MariaDB/0-2-582', time_updated=88822, transaction_timestamp=481828 " +
		"where id=78522"

	got := GenerateUpdatePos(78522, mysql.Position{GTIDSet: gtid.GTIDSet()}, 88822, 481828)
	if got != want {
		t.Errorf("updateVReplicationPos() = %#v, want %#v", got, want)
	}
}

func TestReadVReplicationPos(t *testing.T) {
	want := "select pos from _vt.vreplication where id=482821"
	got := ReadVReplicationPos(482821)
	if got != want {
		t.Errorf("ReadVReplicationPos(482821) = %#v, want %#v", got, want)
	}
}

func TestReadVReplicationStatus(t *testing.T) {
	want := "select pos, state, message from _vt.vreplication where id=482821"
	got := ReadVReplicationStatus(482821)
	if got != want {
		t.Errorf("ReadVReplicationStatus(482821) = %#v, want %#v", got, want)
	}
}
