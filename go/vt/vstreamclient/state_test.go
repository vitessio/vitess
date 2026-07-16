/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

type stateExecuteResponse struct {
	result *sqltypes.Result
	err    error
}

type stateTestVTGateImpl struct {
	testVTGateImpl
	responses []stateExecuteResponse
	queries   []string
	bindVars  []map[string]*querypb.BindVariable

	// rowImage overrides the binlog_row_image probe answer; empty means FULL.
	// rowImageErr makes the probe fail, exercising the warn-and-continue path.
	rowImage    string
	rowImageErr bool
}

func (t *stateTestVTGateImpl) Execute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable, prepared bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	// answered non-positionally so the per-keyspace row-image probe doesn't disturb the
	// positional response queues or query-index assertions
	if strings.Contains(query, "binlog_row_image") {
		if t.rowImageErr {
			return session, nil, errors.New("binlog_row_image probe failed")
		}
		rowImage := t.rowImage
		if rowImage == "" {
			rowImage = "FULL"
		}
		return session, sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.binlog_row_image", "varchar"), rowImage), nil
	}

	t.queries = append(t.queries, query)
	t.bindVars = append(t.bindVars, bindVars)

	if len(t.responses) == 0 {
		return session, &sqltypes.Result{RowsAffected: 1}, nil
	}

	response := t.responses[0]
	t.responses = t.responses[1:]
	return session, response.result, response.err
}

func (t *stateTestVTGateImpl) BinlogDumpGTID(context.Context, string, string, topodatapb.TabletType, *topodatapb.TabletAlias, string, uint64, string, uint32) (vtgateconn.BinlogDumpGTIDReader, error) {
	return nil, errors.New("unexpected BinlogDumpGTID call")
}

func newStateTestSession(t *testing.T, responses ...stateExecuteResponse) (*vtgateconn.VTGateSession, *stateTestVTGateImpl) {
	t.Helper()

	impl := &stateTestVTGateImpl{responses: responses}
	conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
		return impl, nil
	}, "")
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	return conn.Session("", nil), impl
}

func newStateTestConn(t *testing.T, responses ...stateExecuteResponse) (*vtgateconn.VTGateConn, *stateTestVTGateImpl) {
	t.Helper()

	impl := &stateTestVTGateImpl{responses: responses}
	conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
		return impl, nil
	}, "")
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	return conn, impl
}

func stateRowResult(latestVGtid, tableConfig, copyCompleted sqltypes.Value) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "latest_vgtid", Type: querypb.Type_JSON},
			{Name: "table_config", Type: querypb.Type_JSON},
			{Name: "copy_completed", Type: querypb.Type_VARCHAR},
		},
		Rows: [][]sqltypes.Value{{latestVGtid, tableConfig, copyCompleted}},
	}
}

func TestNewVGtid_DeduplicatesKeyspaces(t *testing.T) {
	tables := map[string]*TableConfig{
		"t1": {Keyspace: "ks1", Table: "table_a"},
		"t2": {Keyspace: "ks1", Table: "table_b"},
		"t3": {Keyspace: "ks2", Table: "table_c"},
	}
	shardsByKeyspace := map[string][]string{
		"ks1": {"-80", "80-"},
		"ks2": {"0"},
	}

	vgtid, err := newVGtid(tables, shardsByKeyspace)
	require.NoError(t, err)
	require.NotNil(t, vgtid)

	counts := make(map[string]int)
	for _, shardGtid := range vgtid.ShardGtids {
		counts[shardGtid.Keyspace]++
	}

	assert.Equal(t, 2, counts["ks1"])
	assert.Equal(t, 1, counts["ks2"])
	assert.Len(t, vgtid.ShardGtids, 3)
}

func TestNewVGtid_MissingKeyspaceErrors(t *testing.T) {
	tables := map[string]*TableConfig{
		"t1": {Keyspace: "missing", Table: "table_a"},
	}

	_, err := newVGtid(tables, map[string][]string{"ks1": {"0"}})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "keyspace missing not found")
}

func TestGetLatestVGtid_MalformedStoredJSONErrors(t *testing.T) {
	session, _ := newStateTestSession(t, stateExecuteResponse{result: stateRowResult(
		sqltypes.NewVarBinary("not-json"),
		sqltypes.NewVarBinary(`{"t":{"Keyspace":"ks","Table":"t","Query":"select * from t"}}`),
		sqltypes.NewInt64(1),
	)})

	_, _, _, _, err := getLatestVGtid(t.Context(), session, "stream", "ks", "state")
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to unmarshal latest_vgtid")
}

func TestGetLatestVGtid_MalformedCopyCompletedErrors(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	session, _ := newStateTestSession(t, stateExecuteResponse{result: stateRowResult(
		sqltypes.NewVarBinary(string(vgtidJSON)),
		sqltypes.NewVarBinary(`{"t":{"Keyspace":"ks","Table":"t","Query":"select * from t"}}`),
		sqltypes.NewVarBinary("not-a-bool"),
	)})

	_, _, _, _, err = getLatestVGtid(t.Context(), session, "stream", "ks", "state")
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to convert copy_completed to bool")
}

func TestNew_RestartTableConfigMismatchErrors(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	conn, impl := newStateTestConn(
		t,
		shardsAndStateTableResponses(stateRowResult(
			sqltypes.NewVarBinary(string(vgtidJSON)),
			sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t where id < 10"}}`),
			sqltypes.NewInt64(1),
		))...,
	)

	_, err = New(t.Context(), "stream", conn, []TableConfig{{
		Keyspace:        "ks",
		Table:           "t",
		Query:           "select * from t where id >= 10",
		MaxRowsPerFlush: 1,
		DataType:        &testRowSmall{},
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}}, WithStateTable("stateks", "state"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "provided tables do not match stored tables")
	assert.ErrorContains(t, err, "query changed")

	// a failed constructor must not have fenced the running client: no insert or update may
	// have touched the owner token
	for _, query := range impl.queries {
		if strings.HasPrefix(query, "update ") || strings.HasPrefix(query, "insert ") {
			assert.NotContains(t, query, "owner_token")
		}
	}
}

func TestNew_ResumeThenIdleFlushSkipsCheckpointWrite(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	conn, impl := newStateTestConn(
		t,
		shardsAndStateTableResponses(stateRowResult(
			sqltypes.NewVarBinary(string(vgtidJSON)),
			sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t"}}`),
			sqltypes.NewInt64(1),
		))...,
	)

	v, err := New(t.Context(), "stream", conn, []TableConfig{{
		Keyspace:        "ks",
		Table:           "t",
		Query:           "select * from t",
		MaxRowsPerFlush: 1,
		DataType:        &testRowSmall{},
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}}, WithStateTable("stateks", "state"))
	require.NoError(t, err)
	queriesAfterNew := len(impl.queries)

	// an idle stream after resume has no buffered rows and an unchanged vgtid, so a flush
	// (e.g. triggered by a heartbeat after minFlushDuration) must not rewrite the checkpoint:
	// MySQL reports RowsAffected=0 for a no-op update, which updateLatestVGtid treats as an error
	err = v.flush(t.Context(), false)
	require.NoError(t, err)
	assert.Len(t, impl.queries, queriesAfterNew)
}

func newStateTestTableConfig() TableConfig {
	return TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		Query:           "select * from t",
		MaxRowsPerFlush: 1,
		DataType:        &testRowSmall{},
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}
}

// shardsAndStateTableResponses queues the responses New consumes before any state mutation:
// SHOW VITESS_SHARDS, the create-table DDL, and the state row select. Later writes (claim,
// insert, update) fall through to the fake's default RowsAffected=1 response.
func shardsAndStateTableResponses(stateRow *sqltypes.Result) []stateExecuteResponse {
	responses := []stateExecuteResponse{
		{result: &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "shard", Type: querypb.Type_VARCHAR}},
			Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("ks/0")}, {sqltypes.NewVarBinary("stateks/0")}},
		}},
		{result: &sqltypes.Result{RowsAffected: 1}}, // create state table
	}

	if stateRow == nil {
		stateRow = &sqltypes.Result{}
	}
	return append(responses, stateExecuteResponse{result: stateRow})
}

func TestNew_ExplicitStartingVGtidPersistsWithCopyCompleted(t *testing.T) {
	conn, impl := newStateTestConn(t, shardsAndStateTableResponses(nil)...)

	explicit := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/42"}},
	}

	v, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"), WithStartingVGtid(explicit))
	require.NoError(t, err)

	// the caller provided a starting point and no state row exists, so a single insert persists
	// the explicit vgtid together with copy_completed, and no copy phase runs, now or on restart
	require.Len(t, impl.queries, 4)
	assert.Contains(t, impl.queries[3], "insert into `stateks`.`state`")
	assert.Equal(t, []byte("1"), impl.bindVars[3]["copy_completed"].Value)

	expectedVGtidJSON, err := protojson.Marshal(explicit)
	require.NoError(t, err)
	assert.Equal(t, string(expectedVGtidJSON), string(impl.bindVars[3]["latest_vgtid"].Value))

	// the client stores a clone of the caller-owned vgtid
	assert.NotSame(t, explicit, v.latestVgtid)
	assert.True(t, proto.Equal(explicit, v.latestVgtid))
	assert.Same(t, v.latestVgtid, v.lastFlushedVgtid)
}

func TestNew_ExplicitStartingVGtidOverridesStoredState(t *testing.T) {
	storedVGtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	// the stored table config does not match the provided one; an ordinary resume would fail
	// validation, but an explicit starting vgtid overwrites stored state instead
	conn, impl := newStateTestConn(t, shardsAndStateTableResponses(stateRowResult(
		sqltypes.NewVarBinary(string(storedVGtidJSON)),
		sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t where id < 10"}}`),
		sqltypes.NewInt64(1),
	))...)

	explicit := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/42"}},
	}

	v, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"), WithStartingVGtid(explicit))
	require.NoError(t, err)

	// the row exists, so the explicit position lands via claim + owner-predicated update
	require.Len(t, impl.queries, 5)
	assert.Contains(t, impl.queries[3], "set owner_token = :owner_token")
	assert.Contains(t, impl.queries[4], "update `stateks`.`state`")
	assert.Contains(t, impl.queries[4], "owner_token = :owner_token")
	assert.Equal(t, []byte("1"), impl.bindVars[4]["copy_completed"].Value)
	expectedVGtidJSON, err := protojson.Marshal(explicit)
	require.NoError(t, err)
	assert.Equal(t, string(expectedVGtidJSON), string(impl.bindVars[4]["latest_vgtid"].Value))
	assert.True(t, proto.Equal(explicit, v.latestVgtid))
}

func TestNew_RestartsIncompleteCopyFromScratch(t *testing.T) {
	storedVGtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	// stored state exists but copy_completed is false, so the copy must restart from the
	// beginning instead of resuming from the stored vgtid
	conn, impl := newStateTestConn(t, shardsAndStateTableResponses(stateRowResult(
		sqltypes.NewVarBinary(string(storedVGtidJSON)),
		sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t"}}`),
		sqltypes.NewInt64(0),
	))...)

	v, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"))
	require.NoError(t, err)

	// the row exists, so the fresh copy position lands via claim + owner-predicated update,
	// and copy_completed is explicitly reset so a crash mid-copy is never mistaken for
	// completed state
	require.Len(t, impl.queries, 5)
	assert.Contains(t, impl.queries[3], "set owner_token = :owner_token")
	assert.Contains(t, impl.queries[4], "update `stateks`.`state`")
	assert.Equal(t, []byte("0"), impl.bindVars[4]["copy_completed"].Value)

	freshVGtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: ""}},
	}
	expectedVGtidJSON, err := protojson.Marshal(freshVGtid)
	require.NoError(t, err)
	assert.Equal(t, string(expectedVGtidJSON), string(impl.bindVars[4]["latest_vgtid"].Value))

	require.Len(t, v.latestVgtid.ShardGtids, 1)
	assert.Empty(t, v.latestVgtid.ShardGtids[0].Gtid)
	assert.Same(t, v.latestVgtid, v.lastFlushedVgtid)
}

func TestNew_ClaimsStateOwnershipAfterValidatingState(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	conn, impl := newStateTestConn(
		t,
		shardsAndStateTableResponses(stateRowResult(
			sqltypes.NewVarBinary(string(vgtidJSON)),
			sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t"}}`),
			sqltypes.NewInt64(1),
		))...,
	)

	v, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"))
	require.NoError(t, err)

	// the ownership claim must run after state is read and validated, so a constructor that
	// fails validation can never fence a healthy running client
	require.Len(t, impl.queries, 4)
	assert.Contains(t, impl.queries[2], "select latest_vgtid")
	assert.Contains(t, impl.queries[3], "set owner_token = :owner_token")
	claimToken := impl.bindVars[3]["owner_token"].Value
	assert.NotEmpty(t, claimToken)

	// checkpoint writes must carry the same token the client claimed with
	v.latestVgtid = &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/2"}},
	}
	err = v.flush(t.Context(), false)
	require.NoError(t, err)

	lastIdx := len(impl.queries) - 1
	assert.Contains(t, impl.queries[lastIdx], "owner_token = :owner_token")
	assert.Equal(t, claimToken, impl.bindVars[lastIdx]["owner_token"].Value)
}

func TestNew_RejectsNonFullBinlogRowImage(t *testing.T) {
	conn, impl := newStateTestConn(t, shardsAndStateTableResponses(nil)...)
	impl.rowImage = "NOBLOB"

	_, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"))
	require.ErrorContains(t, err, "requires FULL")
}

func TestNew_UnqueryableBinlogRowImageWarnsAndContinues(t *testing.T) {
	conn, impl := newStateTestConn(t, shardsAndStateTableResponses(nil)...)
	impl.rowImageErr = true

	_, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"))
	require.NoError(t, err)
}

func TestNew_NilTableConfigEntryReportsStructuredError(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	// a null entry is valid JSON; it must surface as a state-validation error, not a panic
	conn, _ := newStateTestConn(
		t,
		shardsAndStateTableResponses(stateRowResult(
			sqltypes.NewVarBinary(string(vgtidJSON)),
			sqltypes.NewVarBinary(`{"ks.t":null}`),
			sqltypes.NewInt64(1),
		))...,
	)

	_, err = New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()},
		WithStateTable("stateks", "state"))
	require.ErrorContains(t, err, "provided tables do not match stored tables")
}

func TestHandleEvents_HeartbeatMidTransactionDefersFlush(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	flushed := 0
	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 10,
		FlushFn: func(_ context.Context, rows []Row, _ FlushMeta) error {
			flushed += len(rows)
			return nil
		},
		currentBatch: []Row{{Data: "buffered"}},
	}

	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session: session,
		stats:   VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		tables:  map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/2"}},
	}

	// with TransactionChunkSize enabled, a heartbeat can arrive between a BEGIN and its COMMIT;
	// flushing there would expose uncommitted rows and checkpoint a transaction prefix
	err := v.handleEvents(t.Context(), []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_BEGIN},
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: vgtid},
		{Type: binlogdatapb.VEventType_HEARTBEAT},
	})
	require.NoError(t, err)
	assert.Zero(t, flushed)
	assert.Empty(t, impl.queries)

	// the terminating COMMIT flushes the buffered rows and checkpoints
	err = v.handleEvents(t.Context(), []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, flushed)
	require.Len(t, impl.queries, 1)

	// once the transaction has terminated, heartbeats flush again as usual
	v.latestVgtid = &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/3"}},
	}
	v.stats.LastFlushedAt = time.Now().Add(-2 * time.Second)
	err = v.handleEvents(t.Context(), []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_HEARTBEAT},
	})
	require.NoError(t, err)
	assert.Len(t, impl.queries, 2)
}

func TestUpdateLatestVGtid_MissingStateRowErrors(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 0}})

	err := updateLatestVGtid(t.Context(), session, "stream", "ks", "state", "token-a", &binlogdatapb.VGtid{}, false)
	require.ErrorIs(t, err, ErrFenced)
	require.Len(t, impl.queries, 1)
	assert.NotContains(t, impl.queries[0], "copy_completed = true")
	assert.Contains(t, impl.queries[0], "owner_token = :owner_token")
	assert.Equal(t, []byte("token-a"), impl.bindVars[0]["owner_token"].Value)
}

func TestHandleEvents_FinalCopyCompletedPersistsCheckpointAndCopyCompletedTogether(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
		},
		session:     session,
		latestVgtid: vgtid,
		tables:      map[string]*TableConfig{},
	}

	err := v.handleEvents(t.Context(), []*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_COPY_COMPLETED}})
	require.NoError(t, err)
	require.Len(t, impl.queries, 1)
	assert.True(t, strings.Contains(impl.queries[0], "latest_vgtid = :latest_vgtid") && strings.Contains(impl.queries[0], "copy_completed = true"))
	assert.Same(t, vgtid, v.lastFlushedVgtid)
	expectedVGtidJSON, err := protojson.Marshal(vgtid)
	require.NoError(t, err)
	assert.Equal(t, string(expectedVGtidJSON), string(impl.bindVars[0]["latest_vgtid"].Value))
}

func TestHandleEvents_HeartbeatCheckpointsLatestVGtidWithoutRows(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session: session,
		tables:  map[string]*TableConfig{},
		stats:   VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
	}

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/2"}},
	}

	err := v.handleEvents(t.Context(), []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: vgtid},
		{Type: binlogdatapb.VEventType_HEARTBEAT},
	})
	require.NoError(t, err)
	require.Len(t, impl.queries, 1)
	assert.Contains(t, impl.queries[0], "update ks.state set latest_vgtid = :latest_vgtid")
	assert.Same(t, vgtid, v.lastFlushedVgtid)
	expectedVGtidJSON, err := protojson.Marshal(vgtid)
	require.NoError(t, err)
	assert.Equal(t, string(expectedVGtidJSON), string(impl.bindVars[0]["latest_vgtid"].Value))
}

func TestUpdateLatestVGtid_CopyCompletedMissingStateRowErrors(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 0}})

	err := updateLatestVGtid(t.Context(), session, "stream", "ks", "state", "token-a", &binlogdatapb.VGtid{}, true)
	require.ErrorIs(t, err, ErrFenced)
	require.Len(t, impl.queries, 1)
	assert.Contains(t, impl.queries[0], "copy_completed = true")
	assert.Contains(t, impl.queries[0], "owner_token = :owner_token")
}
