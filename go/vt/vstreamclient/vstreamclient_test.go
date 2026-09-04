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

package vstreamclient

import (
	"context"
	"errors"
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
	"github.com/stretchr/testify/require"
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
	return nil, nil, errors.New("unexpected Execute call")
}

func (t *testVTGateImpl) ExecuteBatch(context.Context, *vtgatepb.Session, []string, []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return nil, nil, errors.New("unexpected ExecuteBatch call")
}

func (t *testVTGateImpl) StreamExecute(context.Context, *vtgatepb.Session, string, map[string]*querypb.BindVariable, func(*vtgatepb.StreamExecuteResponse)) (sqltypes.ResultStream, error) {
	return nil, errors.New("unexpected StreamExecute call")
}

func (t *testVTGateImpl) ExecuteMulti(context.Context, *vtgatepb.Session, string) (*vtgatepb.Session, []*sqltypes.Result, error) {
	return nil, nil, errors.New("unexpected ExecuteMulti call")
}

func (t *testVTGateImpl) StreamExecuteMulti(context.Context, *vtgatepb.Session, string, func(*vtgatepb.StreamExecuteMultiResponse)) (sqltypes.MultiResultStream, error) {
	return nil, errors.New("unexpected StreamExecuteMulti call")
}

func (t *testVTGateImpl) Prepare(context.Context, *vtgatepb.Session, string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	return nil, nil, 0, errors.New("unexpected Prepare call")
}

func (t *testVTGateImpl) CloseSession(context.Context, *vtgatepb.Session) error {
	return errors.New("unexpected CloseSession call")
}

func (t *testVTGateImpl) VStream(context.Context, topodatapb.TabletType, *binlogdatapb.VGtid, *binlogdatapb.Filter, *vtgatepb.VStreamFlags) (vtgateconn.VStreamReader, error) {
	return t.reader, nil
}

func (t *testVTGateImpl) BinlogDumpGTID(context.Context, string, string, topodatapb.TabletType, *topodatapb.TabletAlias, string, uint64, string, uint32) (vtgateconn.BinlogDumpGTIDReader, error) {
	return nil, errors.New("unexpected BinlogDumpGTID call")
}

func (t *testVTGateImpl) Close() {}

type testVStreamReader struct {
	batches [][]*binlogdatapb.VEvent
	err     error
	index   int
	recvFn  func() ([]*binlogdatapb.VEvent, error)
}

type newTestVTGateImpl struct {
	testVTGateImpl
	shardsByTarget   map[string][]string
	discoveryTargets []string
	vschemaErr       error
	rowImageByTarget map[string]string
}

func (t *newTestVTGateImpl) Execute(_ context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable, prepared bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	switch {
	case query == "SHOW VITESS_SHARDS":
		t.discoveryTargets = append(t.discoveryTargets, session.TargetString)
		if shards, ok := t.shardsByTarget[session.TargetString]; ok {
			return session, sqltypes.MakeTestResult(sqltypes.MakeTestFields("shard", "varchar"), shards...), nil
		}
		return session, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("shard", "varchar"),
			"customer/0",
			"accounting/0",
			"commerce/0",
		), nil

	case strings.HasPrefix(query, "create table if not exists "):
		return session, &sqltypes.Result{RowsAffected: 1}, nil

	case strings.HasPrefix(query, "select latest_vgtid, table_config, copy_completed"):
		return session, &sqltypes.Result{}, nil

	case strings.HasPrefix(query, "insert into "):
		return session, &sqltypes.Result{RowsAffected: 1}, nil

	case strings.HasPrefix(query, "update ") && strings.Contains(query, "owner_token"):
		return session, &sqltypes.Result{RowsAffected: 1}, nil

	case strings.Contains(query, "binlog_row_image"):
		if rowImage, ok := t.rowImageByTarget[session.TargetString]; ok {
			return session, sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.binlog_row_image", "varchar"), rowImage), nil
		}
		return session, sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.binlog_row_image", "varchar"), "FULL"), nil

	case query == "SHOW VSCHEMA KEYSPACES":
		if t.vschemaErr != nil {
			return session, nil, t.vschemaErr
		}
		return session, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Keyspace|Sharded|Foreign Key|Comment", "varchar|varchar|varchar|varchar"),
			"customer|false|unmanaged|",
			"accounting|false|unmanaged|",
			"commerce|false|unmanaged|",
		), nil
	}

	return nil, nil, fmt.Errorf("unexpected Execute call: %s", query)
}

func newConstructorTestConn(t *testing.T) *vtgateconn.VTGateConn {
	t.Helper()

	conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
		return &newTestVTGateImpl{}, nil
	}, "")
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func setLifecycleState(v *VStreamClient, runUsed, runActive, shutdownRequested bool, cancelRunCtxFn context.CancelCauseFunc) {
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
	if r.recvFn != nil {
		return r.recvFn()
	}
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

func TestFlushReason_StringNeverPanics(t *testing.T) {
	assert.Equal(t, "unknown(0)", FlushReason(0).String())
	assert.Equal(t, "unknown(99)", FlushReason(99).String())
	assert.Equal(t, "minDuration", FlushReasonMinDuration.String())
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
	_, err := New(t.Context(), "", nil, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "name is required")

	_, err = New(t.Context(), strings.Repeat("a", 65), nil, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "name must be 64 characters or less")
}

func TestNew_DiscoversShardsForConfiguredTabletType(t *testing.T) {
	for _, tt := range []struct {
		name       string
		tabletType topodatapb.TabletType
		target     string
		shards     []string
		explicit   bool
	}{
		{name: "default replica", target: "@replica", shards: []string{"-80", "80-"}},
		{name: "primary", tabletType: topodatapb.TabletType_PRIMARY, target: "@primary", shards: []string{"0"}},
		{name: "rdonly", tabletType: topodatapb.TabletType_RDONLY, target: "@rdonly", shards: []string{"-40", "40-"}},
		{name: "explicit replica position before tablet option", tabletType: topodatapb.TabletType_REPLICA, target: "@replica", shards: []string{"-80", "80-"}, explicit: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			impl := &newTestVTGateImpl{shardsByTarget: map[string][]string{
				"@primary": {"customer/0", "commerce/0"},
				"@replica": {"customer/-80", "customer/80-"},
				"@rdonly":  {"customer/-40", "customer/40-"},
			}}
			conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
				return impl, nil
			}, "")
			require.NoError(t, err)
			t.Cleanup(conn.Close)

			opts := []Option{WithStateTable("commerce", "state")}
			if tt.explicit {
				position := &binlogdatapb.VGtid{}
				for _, shard := range tt.shards {
					position.ShardGtids = append(position.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: "customer", Shard: shard, Gtid: testConcretePosition})
				}
				opts = append(opts, WithStartingVGtid(position))
			}
			if tt.tabletType != topodatapb.TabletType_UNKNOWN {
				opts = append(opts, WithTabletType(tt.tabletType))
			}
			optionCalls := 0
			opts = append(opts, func(*VStreamClient) error {
				optionCalls++
				return nil
			})
			table := newStateTestTableConfig()
			table.Keyspace = "customer"
			v, err := New(t.Context(), "stream", conn, []TableConfig{table}, opts...)
			require.NoError(t, err)
			assert.Equal(t, []string{tt.target}, impl.discoveryTargets)
			assert.Equal(t, 1, optionCalls)
			shards := make([]string, 0, len(v.latestVgtid.ShardGtids))
			for _, position := range v.latestVgtid.ShardGtids {
				assert.Equal(t, "customer", position.Keyspace)
				shards = append(shards, position.Shard)
			}
			assert.ElementsMatch(t, tt.shards, shards)
		})
	}
}

func TestNew_RejectsNonFullRowImageOnConfiguredTabletType(t *testing.T) {
	for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY} {
		for _, rowImage := range []string{"NOBLOB", "MINIMAL"} {
			t.Run(tabletType.String()+"/"+rowImage, func(t *testing.T) {
				target := "customer:0@" + strings.ToLower(tabletType.String())
				impl := &newTestVTGateImpl{rowImageByTarget: map[string]string{target: rowImage}}
				conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
					return impl, nil
				}, "")
				require.NoError(t, err)
				t.Cleanup(conn.Close)
				table := newStateTestTableConfig()
				table.Keyspace = "customer"
				_, err = New(t.Context(), "stream", conn, []TableConfig{table}, WithStateTable("commerce", "state"), WithTabletType(tabletType))
				require.ErrorContains(t, err, "requires FULL")
				assert.ErrorContains(t, err, target)
			})
		}
	}
}

func TestNew_RequiresStateKeyspaceShardingMetadata(t *testing.T) {
	impl := &newTestVTGateImpl{vschemaErr: errors.New("vschema unavailable")}
	conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
		return impl, nil
	}, "")
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	table := newStateTestTableConfig()
	table.Keyspace = "customer"
	_, err = New(t.Context(), "stream", conn, []TableConfig{table}, WithStateTable("commerce", "state"))
	require.ErrorContains(t, err, "vschema unavailable")
}

func TestMonitorHeartbeat_DoesNotWaitForGracefulShutdown(t *testing.T) {
	for _, tt := range []struct {
		name              string
		startup           bool
		shutdownRequested bool
		cause             error
	}{
		{name: "heartbeat", cause: ErrHeartbeatTimeout},
		{name: "startup", startup: true, cause: ErrStartupTimeout},
		{name: "heartbeat during requested shutdown", shutdownRequested: true, cause: ErrHeartbeatTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				v := &VStreamClient{cfg: clientConfig{
					flags: DefaultFlags(), startupTimeout: 5 * time.Second,
					gracefulShutdownWaitDur:    time.Hour,
					heartbeatTimeoutMultiplier: 2,
				}}
				setLifecycleState(v, true, true, tt.shutdownRequested, cancel)
				t.Cleanup(func() { cancel(nil); v.endRun() })
				if !tt.startup {
					v.lastEventProcessedAtUnixNano = time.Now().Add(-3 * time.Second).UnixNano()
				}
				done := make(chan struct{})
				go func() { defer close(done); v.monitorHeartbeat(ctx) }()
				select {
				case <-done:
					require.ErrorIs(t, context.Cause(ctx), tt.cause)
				case <-time.After(30 * time.Second):
					assert.Fail(t, "liveness cancellation waited for the graceful shutdown window")
				}
			})
		})
	}
}

func TestMonitorHeartbeat_DefaultToleratesShortSilence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn, _ := newStateTestConn(t, shardsAndStateTableResponses(nil)...)
		v, err := New(t.Context(), "stream", conn, []TableConfig{newStateTestTableConfig()}, WithStateTable("stateks", "state"))
		require.NoError(t, err)
		ctx, cancel := context.WithCancelCause(t.Context())
		t.Cleanup(func() { cancel(nil); v.endRun() })
		setLifecycleState(v, true, true, false, cancel)
		v.lastEventProcessedAtUnixNano = time.Now().UnixNano()
		go v.monitorHeartbeat(ctx)

		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.NoError(t, ctx.Err())

		time.Sleep(6 * time.Second)
		synctest.Wait()
		require.ErrorIs(t, context.Cause(ctx), ErrHeartbeatTimeout)
	})
}

func TestMonitorHeartbeat_TimeoutExcludesNewEventBatch(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })
	cancelStarted := make(chan struct{})
	releaseCancel := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseCancel) })
	hookCalled := make(chan struct{}, 1)
	v := &VStreamClient{cfg: clientConfig{flags: DefaultFlags(), heartbeatTimeoutMultiplier: 2, eventFuncs: map[binlogdatapb.VEventType]EventFunc{
		binlogdatapb.VEventType_HEARTBEAT: func(context.Context, *binlogdatapb.VEvent) error {
			hookCalled <- struct{}{}
			return nil
		},
	}}}
	setLifecycleState(v, true, true, false, func(cause error) {
		close(cancelStarted)
		<-releaseCancel
		cancel(cause)
	})
	v.lastEventProcessedAtUnixNano = time.Now().Add(-3 * time.Second).UnixNano()
	go v.monitorHeartbeat(ctx)
	select {
	case <-cancelStarted:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "liveness cancellation did not start")
	}
	eventsDone := make(chan error, 1)
	go func() {
		eventsDone <- v.handleEvents(ctx, []*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_HEARTBEAT}})
	}()
	assert.Never(t, func() bool { return len(hookCalled) != 0 }, 30*time.Second, 10*time.Millisecond)
	releaseOnce.Do(func() { close(releaseCancel) })
	select {
	case err := <-eventsDone:
		require.ErrorIs(t, err, ErrHeartbeatTimeout)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "event processing did not return after cancellation")
	}
	assert.Empty(t, hookCalled)
}

func TestNew_ValidatesMutableDefaults(t *testing.T) {
	t.Run("startup timeout", func(t *testing.T) {
		original := DefaultStartupTimeout
		t.Cleanup(func() { DefaultStartupTimeout = original })
		DefaultStartupTimeout = 0

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultStartupTimeout must be positive")
	})

	t.Run("heartbeat timeout multiplier", func(t *testing.T) {
		original := DefaultHeartbeatTimeoutMultiplier
		t.Cleanup(func() { DefaultHeartbeatTimeoutMultiplier = original })
		DefaultHeartbeatTimeoutMultiplier = -1

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultHeartbeatTimeoutMultiplier must be positive")
	})

	t.Run("negative startup timeout", func(t *testing.T) {
		original := DefaultStartupTimeout
		t.Cleanup(func() { DefaultStartupTimeout = original })
		DefaultStartupTimeout = -time.Second

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultStartupTimeout must be positive")
	})

	t.Run("zero heartbeat timeout multiplier", func(t *testing.T) {
		original := DefaultHeartbeatTimeoutMultiplier
		t.Cleanup(func() { DefaultHeartbeatTimeoutMultiplier = original })
		DefaultHeartbeatTimeoutMultiplier = 0

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultHeartbeatTimeoutMultiplier must be positive")
	})

	t.Run("min flush duration", func(t *testing.T) {
		original := DefaultMinFlushDuration
		t.Cleanup(func() { DefaultMinFlushDuration = original })
		DefaultMinFlushDuration = 0

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultMinFlushDuration must be positive")
	})

	t.Run("graceful shutdown wait", func(t *testing.T) {
		original := DefaultGracefulShutdownWaitDur
		t.Cleanup(func() { DefaultGracefulShutdownWaitDur = original })
		DefaultGracefulShutdownWaitDur = 0

		_, err := New(t.Context(), "stream", newConstructorTestConn(t), nil)
		require.ErrorContains(t, err, "DefaultGracefulShutdownWaitDur must be positive")
	})
}

func TestNew_RejectsHeterogeneousKeyspaceTableSets(t *testing.T) {
	conn := newConstructorTestConn(t)

	_, err := New(
		t.Context(), "test-stream", conn, []TableConfig{
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
				Table:           "invoices",
				Query:           "select * from invoices",
				MaxRowsPerFlush: 1,
				DataType:        &testRowSmall{},
				FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
			},
		},
		WithStateTable("commerce", "vstreams"),
	)
	require.ErrorContains(t, err, "different table/query sets")
}

func TestNew_AllowsIdenticalTableSetsAcrossKeyspaces(t *testing.T) {
	conn := newConstructorTestConn(t)

	_, err := New(
		t.Context(), "test-stream", conn, []TableConfig{
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
	)
	require.NoError(t, err)
}

func TestNew_RejectsAmbiguousBareTableNamesAcrossKeyspaces(t *testing.T) {
	conn := newConstructorTestConn(t)

	_, err := New(
		t.Context(), "test-stream", conn, []TableConfig{
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
	require.Error(t, err)
	require.ErrorContains(t, err, "ExcludeKeyspaceFromTableName")
	assert.ErrorContains(t, err, "customer")
}

func TestWithMinFlushDuration_RejectsNonPositive(t *testing.T) {
	v := &VStreamClient{}
	err := WithMinFlushDuration(0)(v)
	require.Error(t, err)
	assert.ErrorContains(t, err, "minimum flush duration")
}

func TestWithHeartbeatSeconds_RejectsNonPositive(t *testing.T) {
	v := &VStreamClient{}
	err := WithHeartbeatSeconds(0)(v)
	require.Error(t, err)
	assert.ErrorContains(t, err, "heartbeat seconds")
}

func TestWithHeartbeatSeconds_RejectsOverflow(t *testing.T) {
	overflow := uint64(math.MaxUint32) + 1
	if strconv.IntSize < 64 || overflow > uint64(^uint(0)>>1) {
		t.Skip("int cannot represent a value larger than uint32 on this platform")
	}

	v := &VStreamClient{}
	err := WithHeartbeatSeconds(int(overflow))(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "heartbeat seconds must be")
	assert.ErrorContains(t, err, "or less")
}

func TestWithTimeLocation_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithTimeLocation(nil)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "time location")

	loc := time.FixedZone("UTC-5", -5*60*60)
	err = WithTimeLocation(loc)(v)
	require.NoError(t, err)
	assert.Same(t, loc, v.cfg.timeLocation)
}

func TestWithTabletType_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithTabletType(topodatapb.TabletType_UNKNOWN)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "tablet type cannot be UNKNOWN")

	err = WithTabletType(topodatapb.TabletType_RDONLY)(v)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_RDONLY, v.cfg.tabletType)
}

func TestWithFlags_RejectsNil(t *testing.T) {
	v := &VStreamClient{}
	err := WithFlags(nil)(v)
	require.Error(t, err)
	assert.ErrorContains(t, err, "flags cannot be nil")
}

func TestWithGracefulShutdownChan_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithGracefulShutdownChan(nil, time.Second)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "graceful shutdown channel")

	err = WithGracefulShutdownChan(make(chan struct{}), 0)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "graceful shutdown wait")

	ch := make(chan struct{})
	err = WithGracefulShutdownChan(ch, time.Second)(v)
	require.NoError(t, err)
	assert.Equal(t, (<-chan struct{})(ch), v.cfg.gracefulShutdownChan)
	assert.Equal(t, time.Second, v.cfg.gracefulShutdownWaitDur)
}

func TestWithGracefulShutdownSignals_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithGracefulShutdownSignals(time.Second)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "graceful shutdown signals")

	err = WithGracefulShutdownSignals(0, os.Interrupt)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "graceful shutdown wait")

	err = WithGracefulShutdownSignals(time.Second, os.Interrupt)(v)
	require.NoError(t, err)
	assert.Equal(t, []os.Signal{os.Interrupt}, v.cfg.gracefulShutdownSignals)
	assert.Equal(t, time.Second, v.cfg.gracefulShutdownWaitDur)
}

func TestWithEventFunc_Validation(t *testing.T) {
	v := &VStreamClient{}
	fn := func(_ context.Context, _ *binlogdatapb.VEvent) error { return nil }

	err := WithEventFunc(fn)(v)
	require.Error(t, err)
	require.ErrorContains(t, err, "no event types provided")

	err = WithEventFunc(fn, binlogdatapb.VEventType_FIELD)(v)
	require.NoError(t, err)

	err = WithEventFunc(fn, binlogdatapb.VEventType_FIELD)(v)
	require.Error(t, err)
	assert.ErrorContains(t, err, "already has a function")
}

func TestLookupTable(t *testing.T) {
	t.Run("qualified name matches exactly", func(t *testing.T) {
		want := &TableConfig{Keyspace: "ks", Table: "t"}
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): want,
		}}

		got, err := v.lookupTable("ks.t")
		require.NoError(t, err)
		assert.Same(t, want, got)
	})

	t.Run("bare name matches uniquely", func(t *testing.T) {
		want := &TableConfig{Keyspace: "ks", Table: "t"}
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks", "t"): want,
		}}

		got, err := v.lookupTable("t")
		require.NoError(t, err)
		assert.Same(t, want, got)
	})

	t.Run("bare name is rejected when ambiguous", func(t *testing.T) {
		v := &VStreamClient{tables: map[string]*TableConfig{
			qualifiedTableName("ks1", "t"): {Keyspace: "ks1", Table: "t"},
			qualifiedTableName("ks2", "t"): {Keyspace: "ks2", Table: "t"},
		}}

		_, err := v.lookupTable("t")
		require.Error(t, err)
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

	err := v.Run(t.Context())
	require.Error(t, err)
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

	conn, err := vtgateconn.DialCustom(t.Context(), func(context.Context, string) (vtgateconn.Impl, error) {
		return &testVTGateImpl{reader: reader}, nil
	}, "")
	require.NoError(t, err)
	defer conn.Close()

	v := &VStreamClient{
		cfg: clientConfig{
			conn:   conn,
			flags:  DefaultFlags(),
			filter: &binlogdatapb.Filter{},
		},
		tables: map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err = v.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "unexpected EOF")
	assert.Nil(t, v.lastFlushedVgtid)
	require.Len(t, table.currentBatch, 1)
	row, ok := table.currentBatch[0].Data.(*testRowSmall)
	require.True(t, ok)
	assert.Equal(t, int64(7), row.ID)
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

	err := v.flush(t.Context(), false)
	require.NoError(t, err)

	select {
	case <-v.getGracefulShutdownFlushChan():
	default:
		require.FailNow(t, "expected graceful shutdown flush channel to be closed")
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

	err := v.flush(t.Context(), true)
	require.NoError(t, err)
	require.NotNil(t, seenFlushVGtid)
	assert.NotSame(t, v.latestVgtid, seenFlushVGtid)
	require.Len(t, seenFlushVGtid.ShardGtids, 2)
	assert.Equal(t, "mutated", seenFlushVGtid.ShardGtids[0].Gtid)
	require.Len(t, v.latestVgtid.ShardGtids, 1)
	assert.Equal(t, "MySQL56/1", v.latestVgtid.ShardGtids[0].Gtid)
	assert.Same(t, v.latestVgtid, v.lastFlushedVgtid)
}

func TestFlush_ChunksBatchesByMaxRowsPerFlush(t *testing.T) {
	session, impl := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	var chunkSizes []int
	var reasons []FlushReason
	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 2,
		FlushFn: func(_ context.Context, rows []Row, meta FlushMeta) error {
			chunkSizes = append(chunkSizes, len(rows))
			reasons = append(reasons, meta.FlushReason)
			return nil
		},
		currentBatch: []Row{{Data: "1"}, {Data: "2"}, {Data: "3"}, {Data: "4"}, {Data: "5"}},
	}

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session:     session,
		latestVgtid: vgtid,
		stats:       VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		tables:      map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err := v.flush(t.Context(), false)
	require.NoError(t, err)

	assert.Equal(t, []int{2, 2, 1}, chunkSizes)
	assert.Equal(t, []FlushReason{FlushReasonMinDuration, FlushReasonMinDuration, FlushReasonMinDuration}, reasons)
	assert.Empty(t, table.currentBatch)

	assert.Equal(t, 3, table.stats.FlushCount)
	assert.Equal(t, 5, table.stats.FlushedRowCount)
	assert.False(t, table.stats.LastFlushedAt.IsZero())
	assert.Equal(t, 1, v.stats.FlushCount)
	assert.Equal(t, 3, v.stats.TableFlushCount)
	assert.Equal(t, 5, v.stats.FlushedRowCount)

	// all chunks are covered by a single checkpoint write
	assert.Same(t, vgtid, v.lastFlushedVgtid)
	require.Len(t, impl.queries, 1)
	assert.Contains(t, impl.queries[0], "update ks.state set latest_vgtid = :latest_vgtid")
}

func TestFlush_FlushFnErrorStopsFlushAndPreservesState(t *testing.T) {
	session, impl := newStateTestSession(t)

	flushErr := errors.New("sink write failed")
	calls := 0
	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 2,
		FlushFn: func(_ context.Context, _ []Row, _ FlushMeta) error {
			calls++
			if calls == 2 {
				return flushErr
			}
			return nil
		},
		currentBatch: []Row{{Data: "1"}, {Data: "2"}, {Data: "3"}, {Data: "4"}, {Data: "5"}},
	}

	lastFlushedAt := time.Now().Add(-2 * time.Second)
	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session: session,
		latestVgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
		},
		stats:  VStreamStats{LastFlushedAt: lastFlushedAt},
		tables: map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err := v.flush(t.Context(), false)
	require.ErrorIs(t, err, flushErr)
	require.ErrorContains(t, err, "error flushing table t")

	// the batch is preserved for replay, no checkpoint is written, and stream-level flush
	// state is untouched; only the successfully flushed first chunk was counted
	assert.Len(t, table.currentBatch, 5)
	assert.Nil(t, v.lastFlushedVgtid)
	assert.Empty(t, impl.queries)
	assert.Equal(t, 1, table.stats.FlushCount)
	assert.Equal(t, 2, table.stats.FlushedRowCount)
	assert.Equal(t, 0, v.stats.FlushCount)
	assert.Equal(t, lastFlushedAt, v.stats.LastFlushedAt)
}

func TestFlush_ReuseBatchSliceKeepsBackingArrayAcrossFlushes(t *testing.T) {
	session, _ := newStateTestSession(t, stateExecuteResponse{result: &sqltypes.Result{RowsAffected: 1}})

	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 4,
		ReuseBatchSlice: true,
		FlushFn:         func(context.Context, []Row, FlushMeta) error { return nil },
	}
	table.resetBatch()
	table.currentBatch = append(table.currentBatch, Row{Data: "1"}, Row{Data: "2"})
	held := &table.currentBatch[0]

	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session: session,
		latestVgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
		},
		stats:  VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		tables: map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err := v.flush(t.Context(), false)
	require.NoError(t, err)

	require.Empty(t, table.currentBatch)
	table.currentBatch = append(table.currentBatch, Row{Data: "3"})
	assert.Same(t, held, &table.currentBatch[0])
}

func TestFlush_SkipsCheckpointWhenVGtidUnchanged(t *testing.T) {
	session, impl := newStateTestSession(t)

	flushed := 0
	table := &TableConfig{
		Keyspace:        "ks",
		Table:           "t",
		MaxRowsPerFlush: 2,
		FlushFn: func(_ context.Context, rows []Row, _ FlushMeta) error {
			flushed += len(rows)
			return nil
		},
		currentBatch: []Row{{Data: "1"}},
	}

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	v := &VStreamClient{
		cfg: clientConfig{
			name:               "stream",
			vgtidStateKeyspace: "ks",
			vgtidStateTable:    "state",
			minFlushDuration:   time.Second,
		},
		session:          session,
		latestVgtid:      vgtid,
		lastFlushedVgtid: proto.Clone(vgtid).(*binlogdatapb.VGtid),
		stats:            VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		tables:           map[string]*TableConfig{qualifiedTableName("ks", "t"): table},
	}

	err := v.flush(t.Context(), false)
	require.NoError(t, err)

	// buffered rows still flush, but the no-op checkpoint update is skipped, since MySQL
	// reports RowsAffected=0 for an update that changes nothing
	assert.Equal(t, 1, flushed)
	assert.Empty(t, table.currentBatch)
	assert.Empty(t, impl.queries)
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
		// deliberately not t.Context(): contexts created outside the synctest bubble
		// don't participate in its fake-time blocking semantics
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)

		v := &VStreamClient{}
		setLifecycleState(v, true, true, false, cancel)
		defer v.endRun()

		done := make(chan struct{})
		go func() {
			defer close(done)
			v.GracefulShutdown(5 * time.Second)
		}()

		synctest.Wait()
		assert.True(t, v.ShutdownRequested())
		require.NoError(t, ctx.Err())

		time.Sleep(5 * time.Second)
		synctest.Wait()

		select {
		case <-done:
		default:
			require.FailNow(t, "GracefulShutdown did not return after wait elapsed")
		}
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestMonitorHeartbeat_DoesNotShutdownBeforeFirstEvent(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

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
	require.NoError(t, ctx.Err())
	assert.False(t, v.ShutdownRequested())

	cancel(nil)
	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 50*time.Millisecond)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.False(t, v.ShutdownRequested())
}

func TestMonitorHeartbeat_ShutsDownWhenHeartbeatStopsAfterFirstEvent(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	v := &VStreamClient{
		cfg: clientConfig{
			flags:                      DefaultFlags(),
			gracefulShutdownWaitDur:    0,
			heartbeatTimeoutMultiplier: 2,
		},
	}
	setLifecycleState(v, true, true, false, cancel)
	v.lastEventProcessedAtUnixNano = time.Now().Add(-3 * time.Second).UnixNano()

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
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(ctx), ErrHeartbeatTimeout)
	assert.True(t, v.ShutdownRequested())
}

func TestMonitorHeartbeat_StartupTimeoutShutsDownWithCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	v := &VStreamClient{
		cfg: clientConfig{
			flags:                      DefaultFlags(),
			gracefulShutdownWaitDur:    0,
			startupTimeout:             100 * time.Millisecond,
			heartbeatTimeoutMultiplier: 2,
		},
	}
	setLifecycleState(v, true, true, false, cancel)

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
	}, 5*time.Second, 50*time.Millisecond)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(ctx), ErrStartupTimeout)
	assert.True(t, v.ShutdownRequested())
}

func TestShouldExitRun_ReturnsMonitorCauseAfterFinalFlush(t *testing.T) {
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	v := &VStreamClient{}
	setLifecycleState(v, true, true, false, cancel)

	// a monitor-initiated shutdown whose final flush succeeds must still surface its cause,
	// or applications that restart only on errors would silently stop consuming
	_, _, ok := v.requestShutdown(ErrHeartbeatTimeout)
	require.True(t, ok)
	v.signalGracefulShutdownFlushed()

	shouldExit, err := v.shouldExitRun(t.Context())
	assert.True(t, shouldExit)
	require.ErrorIs(t, err, ErrHeartbeatTimeout)
}

func TestShouldExitRun_UserShutdownAfterFinalFlushReturnsNil(t *testing.T) {
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	v := &VStreamClient{}
	setLifecycleState(v, true, true, false, cancel)

	_, _, ok := v.requestShutdown(nil)
	require.True(t, ok)
	v.signalGracefulShutdownFlushed()

	shouldExit, err := v.shouldExitRun(t.Context())
	assert.True(t, shouldExit)
	require.NoError(t, err)
}

func TestEndRun_WakesBlockedGracefulShutdownWaiter(t *testing.T) {
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	v := &VStreamClient{}
	setLifecycleState(v, true, true, false, cancel)

	done := make(chan struct{})
	go func() {
		defer close(done)
		v.GracefulShutdown(time.Hour)
	}()

	assert.Eventually(t, v.ShutdownRequested, 30*time.Second, 10*time.Millisecond)

	// once Run has exited, no future flush can close the channel; endRun must wake the waiter
	// instead of leaving it blocked for the full wait duration
	v.endRun()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "GracefulShutdown did not return after endRun")
	}
}

func TestEffectiveStartupTimeout_FlooredAtLivenessWindow(t *testing.T) {
	assert.Equal(t, 10*time.Second, effectiveStartupTimeout(time.Second, 10*time.Second))
	assert.Equal(t, 5*time.Minute, effectiveStartupTimeout(5*time.Minute, 2*time.Second))
}

func TestShouldExitRun_SurfacesCancelCause(t *testing.T) {
	v := &VStreamClient{}
	setLifecycleState(v, true, true, true, nil)

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(ErrHeartbeatTimeout)

	shouldExit, err := v.shouldExitRun(ctx)
	assert.True(t, shouldExit)
	require.ErrorIs(t, err, ErrHeartbeatTimeout)
}

func TestShouldExitRun_PlainCancelReturnsContextError(t *testing.T) {
	v := &VStreamClient{}
	setLifecycleState(v, true, true, true, nil)

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)

	shouldExit, err := v.shouldExitRun(ctx)
	assert.True(t, shouldExit)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrHeartbeatTimeout)
}
