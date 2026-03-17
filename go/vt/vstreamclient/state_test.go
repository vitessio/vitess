package vstreamclient

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
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

func TestUpdateLatestVGtid_CopyCompletedMissingStateRowErrors(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 0}})

	err := updateLatestVGtid(context.Background(), session, "stream", "ks", "state", &binlogdatapb.VGtid{}, true)
	require.Error(t, err)
	assert.ErrorContains(t, err, "unexpected number of rows affected")
	require.Len(t, impl.queries, 1)
	assert.Contains(t, impl.queries[0], "copy_completed = true")
}
