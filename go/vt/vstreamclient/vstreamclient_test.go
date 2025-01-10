package vstreamclient

import (
	"context"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

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

func TestWithTabletType_Validation(t *testing.T) {
	v := &VStreamClient{}

	err := WithTabletType(topodatapb.TabletType_UNKNOWN)(v)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "tablet type cannot be UNKNOWN")

	err = WithTabletType(topodatapb.TabletType_RDONLY)(v)
	assert.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_RDONLY, v.tabletType)
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
	assert.Equal(t, (<-chan struct{})(ch), v.gracefulShutdownChan)
	assert.Equal(t, time.Second, v.gracefulShutdownWaitDur)
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
	assert.Equal(t, []os.Signal{os.Interrupt}, v.gracefulShutdownSignals)
	assert.Equal(t, time.Second, v.gracefulShutdownWaitDur)
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
	v.isClosing.Store(true)

	err := v.Run(context.Background())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "client is closed")
}

func TestFlush_ClosesGracefulShutdownWhenAlreadyFlushed(t *testing.T) {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{Keyspace: "ks", Shard: "0", Gtid: "MySQL56/1"}},
	}
	v := &VStreamClient{
		tables:                    map[string]*TableConfig{},
		latestVgtid:               vgtid,
		lastFlushedVgtid:          proto.Clone(vgtid).(*binlogdatapb.VGtid),
		gracefulShutdownFlushChan: make(chan struct{}),
	}
	v.isClosing.Store(true)

	err := v.flush(context.Background(), false)
	assert.NoError(t, err)

	select {
	case <-v.gracefulShutdownFlushChan:
	default:
		t.Fatal("expected graceful shutdown flush channel to be closed")
	}
}

func TestShouldFlush_ForceBypassesThresholds(t *testing.T) {
	v := &VStreamClient{
		minFlushDuration: time.Hour,
		stats:            VStreamStats{LastFlushedAt: time.Now()},
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
	assert.Equal(t, FlushReasonForced, reason)
}

func TestShouldFlush_ReturnsReason(t *testing.T) {
	t.Run("min flush duration", func(t *testing.T) {
		v := &VStreamClient{
			minFlushDuration: time.Second,
			stats:            VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		}

		shouldFlush, reason := v.shouldFlush(true, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonMinDuration, reason)
	})

	t.Run("rowless checkpoint still uses last flush time", func(t *testing.T) {
		v := &VStreamClient{
			minFlushDuration: time.Second,
			stats:            VStreamStats{LastFlushedAt: time.Now().Add(-2 * time.Second)},
		}

		shouldFlush, reason := v.shouldFlush(false, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonMinDuration, reason)
	})

	t.Run("max rows per flush", func(t *testing.T) {
		v := &VStreamClient{
			minFlushDuration: time.Hour,
			stats:            VStreamStats{LastFlushedAt: time.Now()},
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
			minFlushDuration: time.Hour,
			stats:            VStreamStats{LastFlushedAt: time.Now()},
		}
		v.isClosing.Store(true)

		shouldFlush, reason := v.shouldFlush(false, false)
		assert.True(t, shouldFlush)
		assert.Equal(t, FlushReasonGracefulShutdown, reason)
	})
}

func TestMonitorHeartbeat_ShutsDownWhenNoInitialEventArrives(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v := &VStreamClient{
		flags:                     DefaultFlags(),
		gracefulShutdownFlushChan: make(chan struct{}),
		gracefulShutdownWaitDur:   0,
		cancelRunCtxFn:            cancel,
	}

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
	}, 4*time.Second, 100*time.Millisecond)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.True(t, v.isClosing.Load())
}
