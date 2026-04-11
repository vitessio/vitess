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
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

type testVTGateImpl struct {
	reader vtgateconn.VStreamReader
}

func (t *testVTGateImpl) Execute(context.Context, *vtgatepb.Session, string, map[string]*querypb.BindVariable, bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	return nil, nil, fmt.Errorf("unexpected Execute call")
}

func (t *testVTGateImpl) ExecuteBatch(context.Context, *vtgatepb.Session, []string, []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return nil, nil, fmt.Errorf("unexpected ExecuteBatch call")
}

func (t *testVTGateImpl) StreamExecute(context.Context, *vtgatepb.Session, string, map[string]*querypb.BindVariable, func(*vtgatepb.StreamExecuteResponse)) (sqltypes.ResultStream, error) {
	return nil, fmt.Errorf("unexpected StreamExecute call")
}

func (t *testVTGateImpl) ExecuteMulti(context.Context, *vtgatepb.Session, string) (*vtgatepb.Session, []*sqltypes.Result, error) {
	return nil, nil, fmt.Errorf("unexpected ExecuteMulti call")
}

func (t *testVTGateImpl) StreamExecuteMulti(context.Context, *vtgatepb.Session, string, func(*vtgatepb.StreamExecuteMultiResponse)) (sqltypes.MultiResultStream, error) {
	return nil, fmt.Errorf("unexpected StreamExecuteMulti call")
}

func (t *testVTGateImpl) Prepare(context.Context, *vtgatepb.Session, string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	return nil, nil, 0, fmt.Errorf("unexpected Prepare call")
}

func (t *testVTGateImpl) CloseSession(context.Context, *vtgatepb.Session) error {
	return fmt.Errorf("unexpected CloseSession call")
}

func (t *testVTGateImpl) VStream(context.Context, topodatapb.TabletType, *binlogdatapb.VGtid, *binlogdatapb.Filter, *vtgatepb.VStreamFlags) (vtgateconn.VStreamReader, error) {
	return t.reader, nil
}

func (t *testVTGateImpl) BinlogDumpGTID(context.Context, string, string, topodatapb.TabletType, *topodatapb.TabletAlias, string, uint64, string, uint32) (vtgateconn.BinlogDumpGTIDReader, error) {
	return nil, fmt.Errorf("unexpected BinlogDumpGTID call")
}

func (t *testVTGateImpl) Close() {}

type testVStreamReader struct {
	batches [][]*binlogdatapb.VEvent
	err     error
	index   int
}

type newTestVTGateImpl struct {
	testVTGateImpl
}

func (t *newTestVTGateImpl) Execute(_ context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable, prepared bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	switch {
	case query == "SHOW VITESS_SHARDS":
		return session, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("shard", "varchar"),
			"customer/0",
			"accounting/0",
			"commerce/0",
		), nil

	case strings.HasPrefix(query, "create table if not exists "):
		return session, &sqltypes.Result{RowsAffected: 1}, nil

	case strings.HasPrefix(query, "select latest_vgtid, table_config, copy_completed from "):
		return session, &sqltypes.Result{}, nil

	case strings.HasPrefix(query, "insert into "):
		return session, &sqltypes.Result{RowsAffected: 1}, nil
	}

	return nil, nil, fmt.Errorf("unexpected Execute call: %s", query)
}

func newConstructorTestConn(t *testing.T) *vtgateconn.VTGateConn {
	t.Helper()

	conn, err := vtgateconn.DialCustom(context.Background(), func(context.Context, string) (vtgateconn.Impl, error) {
		return &newTestVTGateImpl{}, nil
	}, "")
	assert.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func setLifecycleState(v *VStreamClient, runUsed, runActive, shutdownRequested bool, cancelRunCtxFn context.CancelFunc) {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	v.lifecycle.runUsed = runUsed
	v.lifecycle.runActive = runActive
	v.lifecycle.shutdownRequested = shutdownRequested
	v.lifecycle.cancelRunCtxFn = cancelRunCtxFn
	v.lifecycle.gracefulShutdownFlushChan = make(chan struct{})
	v.lifecycle.gracefulShutdownFlushOnce = sync.Once{}
}

func getLifecycleState(v *VStreamClient) (runUsed, runActive, shutdownRequested bool) {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	return v.lifecycle.runUsed, v.lifecycle.runActive, v.lifecycle.shutdownRequested
}

func (r *testVStreamReader) Recv() ([]*binlogdatapb.VEvent, error) {
	if r.index < len(r.batches) {
		batch := r.batches[r.index]
		r.index++
		return batch, nil
	}

	if r.err != nil {
		return nil, r.err
	}

	return nil, io.EOF
}

func TestResolveLatestVGtid(t *testing.T) {
	explicit := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	stored := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/2"}},
	}

	got, explicitUsed := resolveLatestVGtid(explicit, stored)
	assert.True(t, explicitUsed)
	assert.Equal(t, explicit, got)

	got, explicitUsed = resolveLatestVGtid(nil, stored)
	assert.False(t, explicitUsed)
	assert.Equal(t, stored, got)
}

func TestDefaultFlagsExcludeKeyspaceFromTableName(t *testing.T) {
	flags := DefaultFlags()
	assert.False(t, flags.ExcludeKeyspaceFromTableName)
}

func TestNew_ValidatesName(t *testing.T) {
	_, err := New(context.Background(), "", nil, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "name is required")

	_, err = New(context.Background(), strings.Repeat("a", 65), nil, nil)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "name must be 64 characters or less")
}

func TestNew_RejectsAmbiguousBareTableNamesAcrossKeyspaces(t *testing.T) {
	conn := newConstructorTestConn(t)

	_, err := New(context.Background(), "test-stream", conn, []TableConfig{
		{
			Keyspace:        "customer",
			Table:           "customer",
			Query:           "select * from customer where id between 1 and 10",
			MaxRowsPerFlush: 1,
			DataType:        &testRowSmall{},
			FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
		},
		{
			Keyspace:        "accounting",
			Table:           "customer",
			Query:           "select * from customer where id between 1 and 10",
			MaxRowsPerFlush: 1,
			DataType:        &testRowSmall{},
			FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
		},
	},
		WithStateTable("commerce", "vstreams"),
		WithFlags(&vtgatepb.VStreamFlags{HeartbeatInterval: 1, ExcludeKeyspaceFromTableName: true}),
	)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ExcludeKeyspaceFromTableName")
	assert.ErrorContains(t, err, "customer")
}

func TestWithMinFlushDuration_RejectsNonPositive(t *testing.T) {
	v := &VStreamClient{}
	err := WithMinFlushDuration(0)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "minimum flush duration")
}

func TestWithHeartbeatSeconds_RejectsNonPositive(t *testing.T) {
	v := &VStreamClient{}
	err := WithHeartbeatSeconds(0)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "heartbeat seconds")
}

func TestWithHeartbeatSeconds_RejectsOverflow(t *testing.T) {
	overflow := uint64(math.MaxUint32) + 1
	if strconv.IntSize < 64 || overflow > uint64(^uint(0)>>1) {
		t.Skip("int cannot represent a value larger than uint32 on this platform")
	}

	v := &VStreamClient{}
	err := WithHeartbeatSeconds(int(overflow))(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "heartbeat seconds must be")
	assert.ErrorContains(t, err, "or less")
}

func TestWithTimeLocation_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithTimeLocation(nil)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "time location")

	loc := time.FixedZone("UTC-5", -5*60*60)
	err = WithTimeLocation(loc)(v)
	assert.NoError(t, err)
	assert.Same(t, loc, v.cfg.timeLocation)
}

func TestWithTabletType_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithTabletType(topodatapb.TabletType_UNKNOWN)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "tablet type cannot be UNKNOWN")

	err = WithTabletType(topodatapb.TabletType_RDONLY)(v)
	assert.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_RDONLY, v.cfg.tabletType)
}

func TestWithFlags_RejectsNil(t *testing.T) {
	v := &VStreamClient{}
	err := WithFlags(nil)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "flags cannot be nil")
}

func TestWithGracefulShutdownChan_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithGracefulShutdownChan(nil, time.Second)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "graceful shutdown channel")

	err = WithGracefulShutdownChan(make(chan struct{}), 0)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "graceful shutdown wait")

	ch := make(chan struct{})
	err = WithGracefulShutdownChan(ch, time.Second)(v)
	assert.NoError(t, err)
	assert.Equal(t, (<-chan struct{})(ch), v.cfg.gracefulShutdownChan)
	assert.Equal(t, time.Second, v.cfg.gracefulShutdownWaitDur)
}

func TestWithGracefulShutdownSignals_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithGracefulShutdownSignals(time.Second)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "graceful shutdown signals")

	err = WithGracefulShutdownSignals(0, os.Interrupt)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "graceful shutdown wait")

	err = WithGracefulShutdownSignals(time.Second, os.Interrupt)(v)
	assert.NoError(t, err)
	assert.Equal(t, []os.Signal{os.Interrupt}, v.cfg.gracefulShutdownSignals)
	assert.Equal(t, time.Second, v.cfg.gracefulShutdownWaitDur)
}

func TestWithEventFunc_Validation(t *testing.T) {
	v := &VStreamClient{}
	fn := func(_ context.Context, _ *binlogdatapb.VEvent) error { return nil }

	err := WithEventFunc(fn)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "no event types provided")

	err = WithEventFunc(fn, binlogdatapb.VEventType_FIELD)(v)
	assert.NoError(t, err)

	err = WithEventFunc(fn, binlogdatapb.VEventType_FIELD)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "already has a function")
}

func TestLookupTable(t *testing.T) {
	t.Run("qualified name matches exactly", func(t *testing.T) {
		want := &TableConfig{Keyspace: "ks", Table: "t"}
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): want,
		}}

		got, err := v.lookupTable("ks.t")
		assert.NoError(t, err)
		assert.Same(t, want, got)
	})

	t.Run("bare name matches uniquely", func(t *testing.T) {
		want := &TableConfig{Keyspace: "ks", Table: "t"}
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): want,
		}}

		got, err := v.lookupTable("t")
		assert.NoError(t, err)
		assert.Same(t, want, got)
	})

	t.Run("bare name is rejected when ambiguous", func(t *testing.T) {
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks1", "t"): {Keyspace: "ks1", Table: "t"},
			qualifiedTableName("ks2", "t"): {Keyspace: "ks2", Table: "t"},
		}}

		_, err := v.lookupTable("t")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "ambiguous table name")
	})
}

func TestIsFinalCopyCompletedEvent(t *testing.T) {
	t.Run("shard scoped event is not final", func(t *testing.T) {
		assert.False(t, isFinalCopyCompletedEvent(&binlogdatapb.VEvent{
			Type:     binlogdatapb.VEventType_COPY_COMPLETED,
			Keyspace: "ks",
			Shard:    "-80",
		}))
	})

	t.Run("aggregate event is final", func(t *testing.T) {
		assert.True(t, isFinalCopyCompletedEvent(&binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COPY_COMPLETED,
		}))
	})
}

// TestRun_RejectsClosedClient verifies a client cannot be reused after a prior Run attempt.
func TestRun_RejectsClosedClient(t *testing.T) {
	v := &VStreamClient{}
	setLifecycleState(v, true, false, false, nil)

	err := v.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "client is closed")
}

func TestRun_EOFReturnsErrorAndLeavesBufferedRowsUnflushed(t *testing.T) {
	reader := &testVStreamReader{
		batches: [][]*binlogdatapb.VEvent{{
			{
				Type: binlogdatapb.VEventType_FIELD,
				FieldEvent: &binlogdatapb.FieldEvent{
					TableName: "ks.t",
					Shard:     "0",
					Fields: []*querypb.Field{{
						Name: "id",
						Type: querypb.Type_INT64,
					}},
				},
			},
			{
				Type: binlogdatapb.VEventType_ROW,
				RowEvent: &binlogdatapb.RowEvent{
					TableName: "ks.t",
					Shard:     "0",
					RowChanges: []*binlogdatapb.RowChange{{
						After: &querypb.Row{Lengths: []int64{1}, Values: []byte("7")},
					}},
				},
			},
			{
				Type:  binlogdatapb.VEventType_VGTID,
				Vgtid: &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}}},
			},
		}},
		err: io.EOF,
	}

	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		DataType:        &testRowSmall{},
		MaxRowsPerFlush: 10,
		FlushFn: func(context.Context, []Row, FlushMeta) error {
			return nil
		},
		shards: map[string]shardConfig{},
	}
	table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()
	table.resetBatch()

	conn, err := vtgateconn.DialCustom(context.Background(), func(context.Context, string) (vtgateconn.Impl, error) {
		return &testVTGateImpl{reader: reader}, nil
	}, "")
	assert.NoError(t, err)
	defer conn.Close()

	v := &VStreamClient{
		cfg: clientConfig{
			conn:   conn,
			flags:  DefaultFlags(),
			filter: &binlogdatapb.Filter{},
		},
		tables: map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err = v.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unexpected EOF")
	assert.Nil(t, v.lastFlushedVgtid)
	if assert.Len(t, table.currentBatch, 1) {
		row, ok := table.currentBatch[0].Data.(*testRowSmall)
		if assert.True(t, ok) {
			assert.Equal(t, int64(7), row.ID)
		}
	}
}

func TestFlush_ClosesGracefulShutdownWhenAlreadyFlushed(t *testing.T) {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	v := &VStreamClient{
		tables:           map[string]*TableConfig{},
		latestVgtid:      vgtid,
		lastFlushedVgtid: proto.Clone(vgtid).(*binlogdatapb.VGtid),
	}
	setLifecycleState(v, false, false, true, nil)

	err := v.flush(context.Background(), false)
	assert.NoError(t, err)

	select {
	case <-v.getGracefulShutdownFlushChan():
	default:
		t.Fatal("expected graceful shutdown flush channel to be closed")
	}
}

func TestFlush_ConsumerMutationDoesNotAffectInternalCheckpoint(t *testing.T) {
	session, _ := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}

	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 10,
		currentBatch:    []Row{{Data: "row"}},
	}

	var seenFlushVGtid *binlogdatapb.VGtid
	table.FlushFn = func(_ context.Context, _ []Row, meta FlushMeta) error {
		seenFlushVGtid = meta.LatestVGtid
		meta.LatestVGtid.ShardGtids[0].Gtid = "mutated"
		meta.LatestVGtid.ShardGtids = append(meta.LatestVGtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: "ks", Shard: "1", Gtid: "mutated"})
		return nil
	}

	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Hour,
		},
		session:     session,
		latestVgtid: vgtid,
		tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): table,
		},
	}

	err := v.flush(context.Background(), true)
	assert.NoError(t, err)
	assert.NotNil(t, seenFlushVGtid)
	assert.NotSame(t, v.latestVgtid, seenFlushVGtid)
	if assert.Len(t, seenFlushVGtid.ShardGtids, 2) {
		assert.Equal(t, "mutated", seenFlushVGtid.ShardGtids[0].Gtid)
	}
	if assert.Len(t, v.latestVgtid.ShardGtids, 1) {
		assert.Equal(t, "MySQL56/1", v.latestVgtid.ShardGtids[0].Gtid)
	}
	assert.Same(t, v.latestVgtid, v.lastFlushedVgtid)
}

func TestShouldFlush_ForceBypassesThresholds(t *testing.T) {
	v := &VStreamClient{
		cfg:   clientConfig{minFlushDuration: time.Hour},
		stats: VStreamStats{LastFlushedAt: time.Now()},
		tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): {
				Keyspace:        "ks",
				Table:           "t",
				MaxRowsPerFlush: 10,
				currentBatch:    []Row{{Data: "row"}},
			},
		},
	}

	shouldFlush, reason := v.shouldFlush(true, false)
	assert.False(t, shouldFlush)
	assert.Equal(t, FlushReasonNone, reason)

	shouldFlush, reason = v.shouldFlush(true, true)
	assert.True(t, shouldFlush)
	assert.Equal(t, FlushReasonCopyCompleted, reason)
}

func TestShouldFlush_ReturnsReason(t *testing.T) {
	t.Run("min flush duration", func(t *testing.T) {
		v := &VStreamClient{
			cfg:   clientConfig{minFlushDuration: time.Second},
			stats: VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		}

		shouldFlush, reason := v.shouldFlush(true, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonMinDuration, reason)
	})

	t.Run("rowless checkpoint still uses last flush time", func(t *testing.T) {
		v := &VStreamClient{
			cfg:   clientConfig{minFlushDuration: time.Second},
			stats: VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		}

		shouldFlush, reason := v.shouldFlush(false, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonMinDuration, reason)
	})

	t.Run("max rows per flush", func(t *testing.T) {
		v := &VStreamClient{
			cfg:   clientConfig{minFlushDuration: time.Hour},
			stats: VStreamStats{LastFlushedAt: time.Now()},
			tables: map[string]*TableConfig{
				qualifiedTableName("ks", "t"): {
					Keyspace:        "ks",
					Table:           "t",
					MaxRowsPerFlush: 1,
					currentBatch:    []Row{{Data: "row"}},
				},
			},
		}

		shouldFlush, reason := v.shouldFlush(true, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonMaxRowsPerFlush, reason)
	})

	t.Run("graceful shutdown", func(t *testing.T) {
		v := &VStreamClient{
			cfg:   clientConfig{minFlushDuration: time.Hour},
			stats: VStreamStats{LastFlushedAt: time.Now()},
		}
		setLifecycleState(v, false, false, true, nil)

		shouldFlush, reason := v.shouldFlush(false, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonGracefulShutdown, reason)
	})
}

func TestGracefulShutdown_BeforeRunDoesNothing(t *testing.T) {
	v := &VStreamClient{}

	v.GracefulShutdown(time.Second)

	runUsed, runActive, shutdownRequested := getLifecycleState(v)
	assert.False(t, runUsed)
	assert.False(t, runActive)
	assert.False(t, shutdownRequested)
}

func TestGracefulShutdown_CancelsActiveRunAfterWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		v := &VStreamClient{}
		setLifecycleState(v, true, true, false, cancel)
		defer v.endRun()

		done := make(chan struct{})
		go func() {
			defer close(done)
			v.GracefulShutdown(5 * time.Second)
		}()

		synctest.Wait()
		assert.True(t, v.isShutdownRequested())
		assert.NoError(t, ctx.Err())

		time.Sleep(5 * time.Second)
		synctest.Wait()

		select {
		case <-done:
		default:
			t.Fatal("GracefulShutdown did not return after wait elapsed")
		}
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestMonitorHeartbeat_DoesNotShutdownBeforeFirstEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v := &VStreamClient{
		cfg: clientConfig{
			flags:                   DefaultFlags(),
			gracefulShutdownWaitDur: 0,
		},
	}
	setLifecycleState(v, true, true, false, cancel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		v.monitorHeartbeat(ctx)
	}()

	assert.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2500*time.Millisecond, 100*time.Millisecond)
	assert.NoError(t, ctx.Err())
	assert.False(t, v.isShutdownRequested())

	cancel()
	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 50*time.Millisecond)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.False(t, v.isShutdownRequested())
}

func TestMonitorHeartbeat_ShutsDownWhenHeartbeatStopsAfterFirstEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v := &VStreamClient{
		cfg: clientConfig{
			flags:                   DefaultFlags(),
			gracefulShutdownWaitDur: 0,
		},
	}
	setLifecycleState(v, true, true, false, cancel)
	v.lastEventProcessedAtUnixNano.Store(time.Now().Add(-3 * time.Second).UnixNano())

	done := make(chan struct{})
	go func() {
		defer close(done)
		v.monitorHeartbeat(ctx)
	}()

	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second, 100*time.Millisecond)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.True(t, v.isShutdownRequested())
}
