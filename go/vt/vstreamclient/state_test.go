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
}

func (t *stateTestVTGateImpl) Execute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable, prepared bool) (*vtgatepb.Session, *sqltypes.Result, error) {
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
	conn, err := vtgateconn.DialCustom(context.Background(), func(context.Context, string) (vtgateconn.Impl, error) {
		return impl, nil
	}, "")
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	return conn.Session("", nil), impl
}

func newStateTestConn(t *testing.T, responses ...stateExecuteResponse) (*vtgateconn.VTGateConn, *stateTestVTGateImpl) {
	t.Helper()

	impl := &stateTestVTGateImpl{responses: responses}
	conn, err := vtgateconn.DialCustom(context.Background(), func(context.Context, string) (vtgateconn.Impl, error) {
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
	assert.NoError(t, err)
	if !assert.NotNil(t, vgtid) {
		return
	}

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

	_, _, _, err := getLatestVGtid(context.Background(), session, "stream", "ks", "state")
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

	_, _, _, err = getLatestVGtid(context.Background(), session, "stream", "ks", "state")
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to convert copy_completed to bool")
}

func TestNew_RestartTableConfigMismatchErrors(t *testing.T) {
	vgtidJSON, err := protojson.Marshal(&binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	})
	require.NoError(t, err)

	conn, _ := newStateTestConn(t,
		stateExecuteResponse{result: &sqltypes.Result{
			Fields: []*querypb.Field{{Name: "shard", Type: querypb.Type_VARCHAR}},
			Rows:   [][]sqltypes.Value{{sqltypes.NewVarBinary("ks/0")}},
		}},
		stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}},
		stateExecuteResponse{result: stateRowResult(
			sqltypes.NewVarBinary(string(vgtidJSON)),
			sqltypes.NewVarBinary(`{"ks.t":{"Keyspace":"ks","Table":"t","Query":"select * from t where id < 10"}}`),
			sqltypes.NewInt64(1),
		)},
	)

	_, err = New(context.Background(), "stream", conn, []TableConfig{{
		Keyspace:        "ks",
		Table:           "t",
		Query:           "select * from t where id >= 10",
		MaxRowsPerFlush: 1,
		DataType:        &testRowSmall{},
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}}, WithStateTable("ks", "state"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "provided tables do not match stored tables")
	assert.ErrorContains(t, err, "query changed")
}

func TestUpdateLatestVGtid_MissingStateRowErrors(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 0}})

	err := updateLatestVGtid(context.Background(), session, "stream", "ks", "state", &binlogdatapb.VGtid{}, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "unexpected number of rows affected")
	require.Len(t, impl.queries, 1)
	assert.NotContains(t, impl.queries[0], "copy_completed = true")
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

	err := v.handleEvents(context.Background(), []*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_COPY_COMPLETED}})
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

	err := v.handleEvents(context.Background(), []*binlogdatapb.VEvent{
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

	err := updateLatestVGtid(context.Background(), session, "stream", "ks", "state", &binlogdatapb.VGtid{}, true)
	require.Error(t, err)
	assert.ErrorContains(t, err, "unexpected number of rows affected")
	require.Len(t, impl.queries, 1)
	assert.Contains(t, impl.queries[0], "copy_completed = true")
}
