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

package vtgate

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var mu sync.Mutex

func TestVStreamSkew(t *testing.T) {
	stream := func(conn *sandboxconn.SandboxConn, keyspace, shard string, count, idx int64) {
		vevents := getVEvents(keyspace, shard, count, idx)
		for _, ev := range vevents {
			conn.VStreamCh <- ev
			time.Sleep(time.Duration(idx*100) * time.Millisecond)
		}
	}
	type skewTestCase struct {
		numEventsPerShard    int64
		shard0idx, shard1idx int64
		expectedDelays       int64
	}
	tcases := []*skewTestCase{
		// shard0 events are all attempted to be sent first along with the first event of shard1 due to the increased sleep
		// for shard1 in stream(). Third event and fourth events of shard0 need to wait for shard1 to catch up
		{numEventsPerShard: 4, shard0idx: 1, shard1idx: 2, expectedDelays: 2},

		// no delays if streams are aligned or if only one stream is present
		{numEventsPerShard: 4, shard0idx: 1, shard1idx: 1, expectedDelays: 0},
		{numEventsPerShard: 4, shard0idx: 0, shard1idx: 1, expectedDelays: 0},
		{numEventsPerShard: 4, shard0idx: 1, shard1idx: 0, expectedDelays: 0},
	}
	previousDelays := int64(0)
	if vstreamSkewDelayCount == nil {
		// HACK: without a mutex we are not guaranteed that this will avoid the panic caused by a race
		// between this initialization and the one in vtgate.go
		vstreamSkewDelayCount = stats.NewCounter("VStreamEventsDelayedBySkewAlignment",
			"Number of events that had to wait because the skew across shards was too high")
	}

	cell := "aa"
	for idx, tcase := range tcases {
		t.Run("", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ks := fmt.Sprintf("TestVStreamSkew-%d", idx)
			_ = createSandbox(ks)
			hc := discovery.NewFakeHealthCheck(nil)
			st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
			vsm := newTestVStreamManager(ctx, hc, st, cell)
			vgtid := &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{}}
			want := int64(0)
			var sbc0, sbc1 *sandboxconn.SandboxConn
			if tcase.shard0idx != 0 {
				sbc0 = hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
				addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
				sbc0.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: ks, Gtid: "pos", Shard: "-20"})
				go stream(sbc0, ks, "-20", tcase.numEventsPerShard, tcase.shard0idx)
			}
			if tcase.shard1idx != 0 {
				sbc1 = hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
				addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())
				sbc1.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: ks, Gtid: "pos", Shard: "20-40"})
				go stream(sbc1, ks, "20-40", tcase.numEventsPerShard, tcase.shard1idx)
			}

			vstreamCtx, vstreamCancel := context.WithTimeout(ctx, 1*time.Minute)
			defer vstreamCancel()

			receivedEvents := make([]*binlogdatapb.VEvent, 0)
			err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{MinimizeSkew: true}, func(events []*binlogdatapb.VEvent) error {
				receivedEvents = append(receivedEvents, events...)

				if int64(len(receivedEvents)) == want {
					// Stop streaming after receiving both expected responses.
					vstreamCancel()
				}

				return nil
			})

			require.Error(t, err)
			require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

			require.Equal(t, int(want), int(len(receivedEvents)))
			require.Equal(t, tcase.expectedDelays, vsm.GetTotalStreamDelay()-previousDelays)
			previousDelays = vsm.GetTotalStreamDelay()
		})
	}
}

func TestVStreamEventsExcludeKeyspaceFromTableName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})

	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		// Verify that the table names lack the keyspace
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_DDL},
	}
	want2 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid02",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send2, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedResponses := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{ExcludeKeyspaceFromTableName: true}, func(events []*binlogdatapb.VEvent) error {
		receivedResponses = append(receivedResponses, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedResponses) == 2 {
			// Stop streaming after receiving both expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.ElementsMatch(t, []*binlogdatapb.VStreamResponse{want1, want2}, receivedResponses)
}

func TestVStreamEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})

	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_DDL},
	}
	want2 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid02",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send2, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedEvents := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedEvents = append(receivedEvents, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedEvents) == 2 {
			// Stop streaming after receiving both expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.ElementsMatch(t, []*binlogdatapb.VStreamResponse{want1, want2}, receivedEvents)
}

func BenchmarkVStreamEvents(b *testing.B) {
	tests := []struct {
		name                         string
		excludeKeyspaceFromTableName bool
	}{
		{"ExcludeKeyspaceFromTableName=true", true},
		{"ExcludeKeyspaceFromTableName=false", false},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			var f *os.File
			var err error
			if os.Getenv("PROFILE_CPU") == "true" {
				f, err = os.Create("cpu.prof")
				if err != nil {
					b.Fatal(err)
				}
				defer f.Close()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cell := "aa"
			ks := "TestVStream"
			_ = createSandbox(ks)
			hc := discovery.NewFakeHealthCheck(nil)
			st := getSandboxTopo(ctx, cell, ks, []string{"-20"})

			vsm := newTestVStreamManager(ctx, hc, st, cell)
			sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
			addTabletToSandboxTopo(b, ctx, st, ks, "-20", sbc0.Tablet())

			const totalEvents = 100_000
			batchSize := 10_000
			for i := 0; i < totalEvents; i += batchSize {
				var events []*binlogdatapb.VEvent
				events = append(events, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: fmt.Sprintf("gtid-%d", i),
				})
				for j := 0; j < batchSize-2; j++ {
					events = append(events, &binlogdatapb.VEvent{
						Type: binlogdatapb.VEventType_ROW,
						RowEvent: &binlogdatapb.RowEvent{
							TableName: fmt.Sprintf("t%d", j),
						},
					})
				}
				events = append(events, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT})
				sbc0.AddVStreamEvents(events, nil)
			}

			vgtid := &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{{
					Keyspace: ks,
					Shard:    "-20",
					Gtid:     "pos",
				}},
			}

			// Start the timer and CPU profile after all setup is done
			b.ResetTimer()
			if os.Getenv("PROFILE_CPU") == "true" {
				pprof.StartCPUProfile(f)
			}

			vstreamCtx, vstreamCancel := context.WithCancel(ctx)
			defer vstreamCancel()

			received := 0
			err = vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{ExcludeKeyspaceFromTableName: tt.excludeKeyspaceFromTableName}, func(events []*binlogdatapb.VEvent) error {
				received += len(events)

				if received >= totalEvents {
					vstreamCancel()
				}

				return nil
			})

			b.Logf("Received events %d, expected total %d", received, totalEvents)
			b.StopTimer()
			if os.Getenv("PROFILE_CPU") == "true" {
				pprof.StopCPUProfile()
			}

			require.Error(b, err)
			require.ErrorIs(b, vterrors.UnwrapAll(err), context.Canceled)

			require.GreaterOrEqual(b, received, totalEvents)
		})
	}
}

// TestVStreamChunks ensures that a transaction that's broken
// into chunks is sent together.
func TestVStreamChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())

	for i := 0; i < 100; i++ {
		sbc0.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_DDL}}, nil)
		sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}}}, nil)
	}
	sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_COMMIT}}, nil)

	rowEncountered := false
	doneCounting := false
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	var rowCount, ddlCount int
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		switch events[0].Type {
		case binlogdatapb.VEventType_ROW:
			if doneCounting {
				t.Errorf("Unexpected event, only expecting DDL: %v", events[0])
				return fmt.Errorf("unexpected event: %v", events[0])
			}
			rowEncountered = true
			rowCount += 1

		case binlogdatapb.VEventType_COMMIT:
			if !rowEncountered {
				t.Errorf("Unexpected event, COMMIT after non-rows: %v", events[0])
				return fmt.Errorf("unexpected event: %v", events[0])
			}
			doneCounting = true

		case binlogdatapb.VEventType_DDL:
			if !doneCounting && rowEncountered {
				t.Errorf("Unexpected event, DDL during ROW events: %v", events[0])
				return fmt.Errorf("unexpected event: %v", events[0])
			}
			ddlCount += 1

		default:
			t.Errorf("Unexpected event: %v", events[0])
			return fmt.Errorf("unexpected event: %v", events[0])
		}

		if rowCount == 100 && ddlCount == 100 {
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 100, rowCount)
	require.Equal(t, 100, ddlCount)
}

func TestVStreamMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(ctx, hc, st, "aa")
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())

	send0 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(send0, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc1.AddVStreamEvents(send1, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedEvents := make([]*binlogdatapb.VEvent, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedEvents = append(receivedEvents, events...)

		if len(receivedEvents) == 4 {
			// Stop streaming after receiving both expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 4, len(receivedEvents))

	var got *binlogdatapb.VGtid
	for _, ev := range receivedEvents {
		if ev.Type == binlogdatapb.VEventType_VGTID {
			got = ev.Vgtid
		}
	}

	want := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "gtid01",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "gtid02",
		}},
	}

	require.ElementsMatch(t, got.ShardGtids, want.ShardGtids)
}

func TestVStreamsMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Use a unique cell to avoid parallel tests interfering with each other's metrics
	cell := "ab"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	vsm.vstreamsCreated.ResetAll()
	vsm.vstreamsLag.ResetAll()
	vsm.vstreamsCount.ResetAll()
	vsm.vstreamsEventsStreamed.ResetAll()
	vsm.vstreamsEndedWithErrors.ResetAll()
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.2", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())

	send0 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 10, CurrentTime: 15 * 1e9},
	}
	sbc0.AddVStreamEvents(send0, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 10, CurrentTime: 17 * 1e9},
	}
	sbc1.AddVStreamEvents(send1, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}

	expectedLabels1 := "TestVStream.-20.PRIMARY"
	expectedLabels2 := "TestVStream.20-40.PRIMARY"

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedResponses := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedResponses = append(receivedResponses, &binlogdatapb.VStreamResponse{Events: events})

		// While the VStream is running, we should see one active stream per shard.
		require.Equal(t, map[string]int64{
			expectedLabels1: 1,
			expectedLabels2: 1,
		}, vsm.vstreamsCount.Counts())

		if len(receivedResponses) == 2 {
			// Stop streaming after receiving both expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 2, len(receivedResponses))

	// After the streams end, the count should go back to zero.
	require.Equal(t, map[string]int64{
		expectedLabels1: 0,
		expectedLabels2: 0,
	}, vsm.vstreamsCount.Counts())

	require.Equal(t, map[string]int64{
		expectedLabels1: 1,
		expectedLabels2: 1,
	}, vsm.vstreamsCreated.Counts())

	require.Equal(t, map[string]int64{
		expectedLabels1: 5,
		expectedLabels2: 7,
	}, vsm.vstreamsLag.Counts())

	require.Equal(t, map[string]int64{
		expectedLabels1: 2,
		expectedLabels2: 2,
	}, vsm.vstreamsEventsStreamed.Counts())

	require.Equal(t, map[string]int64{
		expectedLabels1: 0,
		expectedLabels2: 0,
	}, vsm.vstreamsEndedWithErrors.Counts())
}

func TestVStreamsMetricsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a unique cell to avoid parallel tests interfering with each other's metrics
	cell := "ac"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	vsm.vstreamsCreated.ResetAll()
	vsm.vstreamsLag.ResetAll()
	vsm.vstreamsCount.ResetAll()
	vsm.vstreamsEventsStreamed.ResetAll()
	vsm.vstreamsEndedWithErrors.ResetAll()
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.2", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())

	const wantErr = "Invalid arg message"
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, wantErr))

	expectedEvents := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 10, CurrentTime: 17 * 1e9},
	}
	sbc1.AddVStreamEvents(expectedEvents, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	results := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		results = append(results, &binlogdatapb.VStreamResponse{Events: events})

		if len(results) == 2 {
			// We should never actually see 2 responses come in
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorContains(t, err, wantErr)

	// Because there's essentially a race condition between the two streams,
	// we may get 0 or 1 results, depending on whether the error from
	// sbc0 or the events from sbc1 come first.
	require.LessOrEqual(t, len(results), 1)
	if len(results) == 1 {
		require.Len(t, results[0].Events, 2)
	}

	// When we verify the metrics, we should see that the -20 stream had an error,
	// while the 20-40 stream might have one too (if the error from -20 came first),
	// or might not (if the events from 20-40 came first).
	// So we only verify the -20 metrics exactly, while the 20-40 metrics are
	// verified to be at least 0 or 1 as appropriate.

	errorCounts := vsm.vstreamsEndedWithErrors.Counts()
	require.Contains(t, errorCounts, "TestVStream.-20.PRIMARY")
	require.Contains(t, errorCounts, "TestVStream.20-40.PRIMARY")

	require.Equal(t, int64(1), errorCounts["TestVStream.-20.PRIMARY"])
	require.LessOrEqual(t, errorCounts["TestVStream.20-40.PRIMARY"], int64(1))
}

func TestVStreamErrorInCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a unique cell to avoid parallel tests interfering with each other's metrics
	cell := "ac"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	vsm.vstreamsCreated.ResetAll()
	vsm.vstreamsLag.ResetAll()
	vsm.vstreamsCount.ResetAll()
	vsm.vstreamsEventsStreamed.ResetAll()
	vsm.vstreamsEndedWithErrors.ResetAll()
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.2", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "20-40", sbc1.Tablet())

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 10, CurrentTime: 15 * 1e9},
	}
	sbc0.AddVStreamEvents(send1, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT, Timestamp: 10, CurrentTime: 17 * 1e9},
	}
	sbc1.AddVStreamEvents(send2, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: ks,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}

	expectedError := fmt.Errorf("callback error")

	err := vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		return expectedError
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), expectedError)
}

func TestVStreamRetriableErrors(t *testing.T) {
	type testCase struct {
		name         string
		code         vtrpcpb.Code
		msg          string
		shouldRetry  bool
		ignoreTablet bool
	}

	tcases := []testCase{
		{
			name:         "failed precondition",
			code:         vtrpcpb.Code_FAILED_PRECONDITION,
			msg:          "",
			shouldRetry:  true,
			ignoreTablet: false,
		},
		{
			name:         "gtid mismatch",
			code:         vtrpcpb.Code_INVALID_ARGUMENT,
			msg:          "GTIDSet Mismatch aa",
			shouldRetry:  true,
			ignoreTablet: true,
		},
		{
			name:         "unavailable",
			code:         vtrpcpb.Code_UNAVAILABLE,
			msg:          "",
			shouldRetry:  true,
			ignoreTablet: false,
		},
		{
			name:         "invalid argument",
			code:         vtrpcpb.Code_INVALID_ARGUMENT,
			msg:          "final error",
			shouldRetry:  false,
			ignoreTablet: false,
		},
		{
			name:         "query interrupted",
			code:         vtrpcpb.Code_UNKNOWN,
			msg:          "vttablet: rpc error: code = Unknown desc = Query execution was interrupted, maximum statement execution time exceeded (errno 3024) (sqlstate HY000)",
			shouldRetry:  true,
			ignoreTablet: false,
		},
	}

	commit := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// aa will be the local cell for this test, but that tablet will have a vstream error.
			cells := []string{"aa", "ab"}

			ks := "TestVStream"
			_ = createSandbox(ks)
			hc := discovery.NewFakeHealthCheck(nil)

			st := getSandboxTopoMultiCell(ctx, cells, ks, []string{"-20"})

			sbc0 := hc.AddTestTablet(cells[0], "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_REPLICA, true, 1, nil)
			sbc1 := hc.AddTestTablet(cells[1], "1.1.1.1", 1002, ks, "-20", topodatapb.TabletType_REPLICA, true, 1, nil)

			addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
			addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc1.Tablet())

			vsm := newTestVStreamManager(ctx, hc, st, cells[0])

			// Always have the local cell tablet error so it's ignored on retry and we pick the other one
			// if the error requires ignoring the tablet on retry.
			sbc0.AddVStreamEvents(nil, vterrors.New(tcase.code, tcase.msg))

			if tcase.ignoreTablet {
				sbc1.AddVStreamEvents(commit, nil)
			} else {
				sbc0.AddVStreamEvents(commit, nil)
			}

			vgtid := &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{{
					Keyspace: ks,
					Shard:    "-20",
					Gtid:     "pos",
				}},
			}

			vstreamCtx, vstreamCancel := context.WithCancel(ctx)
			defer vstreamCancel()

			err := vsm.VStream(vstreamCtx, topodatapb.TabletType_REPLICA, vgtid, nil, &vtgatepb.VStreamFlags{Cells: strings.Join(cells, ",")}, func(events []*binlogdatapb.VEvent) error {
				defer vstreamCancel()

				require.Equal(t, 1, len(events))
				require.Equal(t, commit, events)

				return nil
			})

			if tcase.shouldRetry {
				// Expect a cancel error because the stream was retried and our callback
				// was called.
				require.Error(t, err)
				require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)
			} else {
				// Expect the original error because no retry was done.
				require.Error(t, err)
				require.ErrorContains(t, err, tcase.msg)
			}
		})
	}
}

func TestVStreamShouldNotSendSourceHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())

	send0 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_HEARTBEAT},
	}
	sbc0.AddVStreamEvents(send0, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_HEARTBEAT},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_HEARTBEAT},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedResponses := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedResponses = append(receivedResponses, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedResponses) == 1 {
			// Stop streaming after receiving the expected response.
			vstreamCancel()
		}
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 1, len(receivedResponses))
	require.EqualExportedValues(t, want, receivedResponses[0])
}

func TestVStreamJournalOneToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "-10", "10-20"})
	vsm := newTestVStreamManager(ctx, hc, st, "aa")
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet(cell, "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "10-20", sbc2.Tablet())

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-10",
				Gtid:     "pos10",
			}, {
				Keyspace: ks,
				Shard:    "10-20",
				Gtid:     "pos1020",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: ks,
				Shard:    "-20",
			}},
		}},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(send2, nil)

	send3 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid03"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t3"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc1.ExpectVStreamStartPos("pos10")
	sbc1.AddVStreamEvents(send3, nil)

	send4 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid04"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t4"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc2.ExpectVStreamStartPos("pos1020")
	sbc2.AddVStreamEvents(send4, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedEvents := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedEvents = append(receivedEvents, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedEvents) == 3 {
			// Stop streaming after receiving all expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 3, len(receivedEvents))

	// First event should be the first transaction from the first shard.
	require.EqualExportedValues(t, want1, receivedEvents[0])

	// The second and third events can come in any order.
	// So instead of comparing them directly, we simply verify that the GTID
	// after the last event is the expected combined GTID.

	require.EqualExportedValues(t, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_VGTID,
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-10",
				Gtid:     "gtid03",
			}, {
				Keyspace: ks,
				Shard:    "10-20",
				Gtid:     "gtid04",
			}},
		},
	}, receivedEvents[2].Events[0])
}

func TestVStreamJournalManyToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Variable names are maintained like in OneToMany, but order is different.
	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "-10", "10-20"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet(cell, "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "10-20", sbc2.Tablet())

	send3 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid03"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t3"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc1.ExpectVStreamStartPos("pos10")
	sbc1.AddVStreamEvents(send3, nil)

	send4 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid04"},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t4"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc2.ExpectVStreamStartPos("pos1020")
	sbc2.AddVStreamEvents(send4, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "pos20",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: ks,
				Shard:    "-10",
			}, {
				Keyspace: ks,
				Shard:    "10-20",
			}},
		}},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	// Journal event has to be sent by both shards.
	sbc1.AddVStreamEvents(send2, nil)
	sbc2.AddVStreamEvents(send2, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.ExpectVStreamStartPos("pos20")
	sbc0.AddVStreamEvents(send1, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-10",
			Gtid:     "pos10",
		}, {
			Keyspace: ks,
			Shard:    "10-20",
			Gtid:     "pos1020",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedResponses := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedResponses = append(receivedResponses, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedResponses) == 3 {
			// Stop streaming after receiving all expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 3, len(receivedResponses))

	require.EqualExportedValues(t, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_VGTID,
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-10",
				Gtid:     "gtid03",
			}, {
				Keyspace: ks,
				Shard:    "10-20",
				Gtid:     "gtid04",
			}},
		},
	}, receivedResponses[1].Events[0])

	require.EqualExportedValues(t, want1, receivedResponses[2])
}

func TestVStreamJournalNoMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(ctx, hc, st, "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid01",
			}},
		}},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	tableJournal := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_TABLES,
		}},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "jn1"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	wantjn1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "jn1",
			}},
		}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(tableJournal, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_DDL},
	}
	want2 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid02",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send2, nil)

	shardJournal := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            2,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "c0-",
				Gtid:     "posc0",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: ks,
				Shard:    "c0-e0",
			}, {
				Keyspace: ks,
				Shard:    "e0-",
			}},
		}},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "jn2"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	wantjn2 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "jn2",
			}},
		}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(shardJournal, nil)

	send3 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid03"},
		{Type: binlogdatapb.VEventType_DDL},
	}
	want3 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "-20",
				Gtid:     "gtid03",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send3, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	vstreamCtx, vstreamCancel := context.WithCancel(ctx)
	defer vstreamCancel()

	receivedResponses := make([]*binlogdatapb.VStreamResponse, 0)
	err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		receivedResponses = append(receivedResponses, &binlogdatapb.VStreamResponse{Events: events})

		if len(receivedResponses) == 5 {
			// Stop streaming after receiving all expected responses.
			vstreamCancel()
		}

		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, vterrors.UnwrapAll(err), context.Canceled)

	require.Equal(t, 5, len(receivedResponses))

	require.EqualExportedValues(t, want1, receivedResponses[0])
	require.EqualExportedValues(t, wantjn1, receivedResponses[1])
	require.EqualExportedValues(t, want2, receivedResponses[2])
	require.EqualExportedValues(t, wantjn2, receivedResponses[3])
	require.EqualExportedValues(t, want3, receivedResponses[4])
}

func TestVStreamJournalPartialMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Variable names are maintained like in OneToMany, but order is different.
	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "-10", "10-20"})
	vsm := newTestVStreamManager(ctx, hc, st, "aa")
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "10-20", sbc2.Tablet())

	send := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "10-30",
				Gtid:     "pos1040",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: ks,
				Shard:    "10-20",
			}, {
				Keyspace: ks,
				Shard:    "20-30",
			}},
		}},
	}
	sbc2.AddVStreamEvents(send, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-10",
			Gtid:     "pos10",
		}, {
			Keyspace: ks,
			Shard:    "10-20",
			Gtid:     "pos1020",
		}},
	}

	err := vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		return fmt.Errorf("unexpected events: %v", events)
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not all journaling participants are in the stream")

	// Try a different order (different code path)
	send = []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: ks,
				Shard:    "10-30",
				Gtid:     "pos1040",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: ks,
				Shard:    "20-30",
			}, {
				Keyspace: ks,
				Shard:    "10-20",
			}},
		}},
	}
	sbc2.AddVStreamEvents(send, nil)

	err = vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		return fmt.Errorf("unexpected events: %v", events)
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "not all journaling participants are in the stream")
}

func TestResolveVStreamParams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck(nil)
	vsm := newTestVStreamManager(ctx, hc, newSandboxForCells(ctx, []string{"aa"}), "aa")
	testcases := []struct {
		input  *binlogdatapb.VGtid
		output *binlogdatapb.VGtid
		err    string
	}{{
		input: nil,
		err:   "vgtid must have at least one value with a starting position",
	}, {
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{}},
		},
		err: "for an empty keyspace, the Gtid value must be 'current'",
	}, {
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Gtid:     "other",
			}},
		},
		err: "if shards are unspecified, the Gtid value must be 'current' or empty",
	}, {
		// Verify that the function maps the input missing the shard to a list of all shards in the topology.
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
			}},
		},
		output: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
			}, {
				Keyspace: "TestVStream",
				Shard:    "20-40",
			}, {
				Keyspace: "TestVStream",
				Shard:    "40-60",
			}, {
				Keyspace: "TestVStream",
				Shard:    "60-80",
			}, {
				Keyspace: "TestVStream",
				Shard:    "80-a0",
			}, {
				Keyspace: "TestVStream",
				Shard:    "a0-c0",
			}, {
				Keyspace: "TestVStream",
				Shard:    "c0-e0",
			}, {
				Keyspace: "TestVStream",
				Shard:    "e0-",
			}},
		},
	}, {
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Gtid:     "current",
			}},
		},
		output: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "20-40",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "40-60",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "60-80",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "80-a0",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "a0-c0",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "c0-e0",
				Gtid:     "current",
			}, {
				Keyspace: "TestVStream",
				Shard:    "e0-",
				Gtid:     "current",
			}},
		},
	}, {
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
				Gtid:     "current",
			}},
		},
		output: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
				Gtid:     "current",
			}},
		},
	}, {
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
				Gtid:     "other",
			}},
		},
		output: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
				Shard:    "-20",
				Gtid:     "other",
			}},
		},
	}}
	wantFilter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/.*",
		}},
	}
	for _, tcase := range testcases {
		vgtid, filter, flags, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, tcase.input, nil, nil)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("resolve(%v) err: %v, must contain %v", tcase.input, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err, tcase.input)
		assert.Equal(t, tcase.output, vgtid, tcase.input)
		assert.Equal(t, wantFilter, filter, tcase.input)
		require.False(t, flags.MinimizeSkew)
	}

	// Special-case: empty keyspace or keyspace containing wildcards because output is too big.
	// Verify that the function resolves input for multiple keyspaces into a list of all corresponding shards.
	// Ensure that the number of shards returned is greater than the number of shards in a single keyspace named 'TestVStream.'
	specialCases := []struct {
		input *binlogdatapb.ShardGtid
	}{
		{
			input: &binlogdatapb.ShardGtid{
				Gtid: "current",
			},
		},
		{
			input: &binlogdatapb.ShardGtid{
				Keyspace: "/.*",
			},
		},
		{
			input: &binlogdatapb.ShardGtid{
				Keyspace: "/.*",
				Gtid:     "current",
			},
		},
		{
			input: &binlogdatapb.ShardGtid{
				Keyspace: "/Test.*",
			},
		},
	}
	for _, tcase := range specialCases {
		input := &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{tcase.input},
		}
		vgtid, _, _, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, input, nil, nil)
		require.NoError(t, err, tcase.input)
		if got, expectTestVStreamShardNumber := len(vgtid.ShardGtids), 8; expectTestVStreamShardNumber >= got {
			t.Errorf("len(vgtid.ShardGtids): %v, must be >%d", got, expectTestVStreamShardNumber)
		}
		for _, s := range vgtid.ShardGtids {
			require.Equal(t, tcase.input.Gtid, s.Gtid)
		}
	}

	for _, minimizeSkew := range []bool{true, false} {
		t.Run(fmt.Sprintf("resolveParams MinimizeSkew %t", minimizeSkew), func(t *testing.T) {
			flags := &vtgatepb.VStreamFlags{MinimizeSkew: minimizeSkew}
			vgtid := &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{{
					Keyspace: "TestVStream",
					Shard:    "-20",
					Gtid:     "current",
				}},
			}
			_, _, flags2, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, vgtid, nil, flags)
			require.NoError(t, err)
			require.Equal(t, minimizeSkew, flags2.MinimizeSkew)
		})
	}

}

func TestVStreamIdleHeartbeat(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, ctx, st, ks, "-20", sbc0.Tablet())
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}

	type testcase struct {
		name              string
		heartbeatInterval uint32
		want              int
	}
	// each test waits for 4.5 seconds, hence expected #heartbeats = floor(4.5/heartbeatInterval)
	testcases := []testcase{
		{"off", 0, 0},
		{"on:1s", 1, 4},
		{"on:2s", 2, 2},
	}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			var heartbeatCount int

			vstreamCtx, vstreamCancel := context.WithTimeout(ctx, time.Duration(4500)*time.Millisecond)
			defer vstreamCancel()

			err := vsm.VStream(vstreamCtx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{HeartbeatInterval: tcase.heartbeatInterval}, func(events []*binlogdatapb.VEvent) error {
				for _, event := range events {
					if event.Type == binlogdatapb.VEventType_HEARTBEAT {
						heartbeatCount++
					}
				}

				return nil
			})

			require.Error(t, err)
			require.ErrorIs(t, vterrors.UnwrapAll(err), context.DeadlineExceeded)

			require.Equalf(t, heartbeatCount, tcase.want, "got %d, want %d", heartbeatCount, tcase.want)
		})
	}
}

func TestKeyspaceHasBeenSharded(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	cell := "zone1"
	ks := "testks"

	type testcase struct {
		name            string
		oldshards       []string
		newshards       []string
		vgtid           *binlogdatapb.VGtid
		trafficSwitched bool
		want            bool
		wantErr         string
	}
	testcases := []testcase{
		{
			name: "2 to 4, split both, traffic not switched",
			oldshards: []string{
				"-80",
				"80-",
			},
			newshards: []string{
				"-40",
				"40-80",
				"80-c0",
				"c0-",
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-",
					},
				},
			},
			trafficSwitched: false,
			want:            false,
		},
		{
			name: "2 to 4, split both, traffic not switched",
			oldshards: []string{
				"-80",
				"80-",
			},
			newshards: []string{
				"-40",
				"40-80",
				"80-c0",
				"c0-",
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-",
					},
				},
			},
			trafficSwitched: false,
			want:            false,
		},
		{
			name: "2 to 8, split both, traffic switched",
			oldshards: []string{
				"-80",
				"80-",
			},
			newshards: []string{
				"-20",
				"20-40",
				"40-60",
				"60-80",
				"80-a0",
				"a0-c0",
				"c0-e0",
				"e0-",
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-",
					},
				},
			},
			trafficSwitched: true,
			want:            true,
		},
		{
			name: "2 to 4, split only first shard, traffic switched",
			oldshards: []string{
				"-80",
				"80-",
			},
			newshards: []string{
				"-20",
				"20-40",
				"40-60",
				"60-80",
				// 80- is not being resharded.
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-",
					},
				},
			},
			trafficSwitched: true,
			want:            true,
		},
		{
			name: "4 to 2, merge both shards, traffic switched",
			oldshards: []string{
				"-40",
				"40-80",
				"80-c0",
				"c0-",
			},
			newshards: []string{
				"-80",
				"80-",
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-40",
					},
					{
						Keyspace: ks,
						Shard:    "40-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-c0",
					},
					{
						Keyspace: ks,
						Shard:    "c0-",
					},
				},
			},
			trafficSwitched: true,
			want:            true,
		},
		{
			name: "4 to 3, merge second half, traffic not switched",
			oldshards: []string{
				"-40",
				"40-80",
				"80-c0",
				"c0-",
			},
			newshards: []string{
				// -40 and 40-80 are not being resharded.
				"80-", // Merge of 80-c0 and c0-
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-40",
					},
					{
						Keyspace: ks,
						Shard:    "40-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-c0",
					},
					{
						Keyspace: ks,
						Shard:    "c0-",
					},
				},
			},
			trafficSwitched: false,
			want:            false,
		},
		{
			name: "4 to 3, merge second half, traffic switched",
			oldshards: []string{
				"-40",
				"40-80",
				"80-c0",
				"c0-",
			},
			newshards: []string{
				// -40 and 40-80 are not being resharded.
				"80-", // Merge of 80-c0 and c0-
			},
			vgtid: &binlogdatapb.VGtid{
				ShardGtids: []*binlogdatapb.ShardGtid{
					{
						Keyspace: ks,
						Shard:    "-40",
					},
					{
						Keyspace: ks,
						Shard:    "40-80",
					},
					{
						Keyspace: ks,
						Shard:    "80-c0",
					},
					{
						Keyspace: ks,
						Shard:    "c0-",
					},
				},
			},
			trafficSwitched: true,
			want:            true,
		},
	}

	addTablet := func(t *testing.T, ctx context.Context, host string, port int32, cell, ks, shard string, ts *topo.Server, hc *discovery.FakeHealthCheck, serving bool) {
		tabletconn := hc.AddTestTablet(cell, host, port, ks, shard, topodatapb.TabletType_PRIMARY, serving, 0, nil)
		err := ts.CreateTablet(ctx, tabletconn.Tablet())
		require.NoError(t, err)
		var alias *topodatapb.TabletAlias
		if serving {
			alias = tabletconn.Tablet().Alias
		}
		_, err = ts.UpdateShardFields(ctx, ks, shard, func(si *topo.ShardInfo) error {
			si.PrimaryAlias = alias
			si.IsPrimaryServing = serving
			return nil
		})
		require.NoError(t, err)
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			hc := discovery.NewFakeHealthCheck(nil)
			_ = createSandbox(ks)
			st := getSandboxTopo(ctx, cell, ks, append(tc.oldshards, tc.newshards...))
			vsm := newTestVStreamManager(ctx, hc, st, cell)
			vs := vstream{
				vgtid:      tc.vgtid,
				tabletType: topodatapb.TabletType_PRIMARY,
				optCells:   cell,
				vsm:        vsm,
				ts:         st.topoServer,
			}
			for i, shard := range tc.oldshards {
				addTablet(t, ctx, fmt.Sprintf("1.1.0.%d", i), int32(1000+i), cell, ks, shard, st.topoServer, hc, !tc.trafficSwitched)
			}
			for i, shard := range tc.newshards {
				addTablet(t, ctx, fmt.Sprintf("1.1.1.%d", i), int32(2000+i), cell, ks, shard, st.topoServer, hc, tc.trafficSwitched)
			}
			got, err := vs.keyspaceHasBeenResharded(ctx, ks)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// TestVStreamManagerHealthCheckResponseHandling tests the handling of healthcheck responses by
// the vstream manager to confirm that we are correctly restarting the vstream when we should.
func TestVStreamManagerHealthCheckResponseHandling(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	// Capture the vstream warning log. Otherwise we need to re-implement the vstream error
	// handling in SandboxConn's implementation and then we're not actually testing the
	// production code.
	logger := logutil.NewMemoryLogger()
	log.Warningf = logger.Warningf

	cell := "aa"
	ks := "TestVStream"
	shard := "0"
	tabletType := topodatapb.TabletType_REPLICA
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{shard})
	vsm := newTestVStreamManager(ctx, hc, st, cell)
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    shard,
		}},
	}
	source := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, shard, tabletType, true, 0, nil)
	tabletAlias := topoproto.TabletAliasString(source.Tablet().Alias)
	addTabletToSandboxTopo(t, ctx, st, ks, shard, source.Tablet())
	target := &querypb.Target{
		Cell:       cell,
		Keyspace:   ks,
		Shard:      shard,
		TabletType: tabletType,
	}
	highLag := uint32(discovery.GetLowReplicationLag().Seconds()) + 1

	type testcase struct {
		name    string
		hcRes   *querypb.StreamHealthResponse
		wantErr string
	}
	testcases := []testcase{
		{
			name: "all healthy", // Will hit the context timeout
		},
		{
			name: "failure",
			hcRes: &querypb.StreamHealthResponse{
				TabletAlias: source.Tablet().Alias,
				Target:      nil, // This is seen as a healthcheck stream failure
			},
			wantErr: fmt.Sprintf("health check failed on %s", tabletAlias),
		},
		{
			name: "tablet type changed",
			hcRes: &querypb.StreamHealthResponse{
				TabletAlias: source.Tablet().Alias,
				Target: &querypb.Target{
					Cell:       cell,
					Keyspace:   ks,
					Shard:      shard,
					TabletType: topodatapb.TabletType_PRIMARY,
				},
				PrimaryTermStartTimestamp: time.Now().Unix(),
				RealtimeStats:             &querypb.RealtimeStats{},
			},
			wantErr: fmt.Sprintf("tablet %s type has changed from %s to %s",
				tabletAlias, tabletType, topodatapb.TabletType_PRIMARY.String()),
		},
		{
			name: "unhealthy",
			hcRes: &querypb.StreamHealthResponse{
				TabletAlias: source.Tablet().Alias,
				Target:      target,
				RealtimeStats: &querypb.RealtimeStats{
					HealthError: "unhealthy",
				},
			},
			wantErr: fmt.Sprintf("tablet %s is no longer healthy", tabletAlias),
		},
		{
			name: "replication lag too high",
			hcRes: &querypb.StreamHealthResponse{
				TabletAlias: source.Tablet().Alias,
				Target:      target,
				RealtimeStats: &querypb.RealtimeStats{
					ReplicationLagSeconds: highLag,
				},
			},
			wantErr: fmt.Sprintf("%s has a replication lag of %d seconds which is beyond the value provided",
				tabletAlias, highLag),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr != "" {
				source.SetStreamHealthResponse(tc.hcRes)
			}

			vstreamCtx, vstreamCancel := context.WithTimeout(ctx, 5*time.Second)
			defer vstreamCancel()

			// SandboxConn's VStream implementation always waits for the context to timeout.
			err := vsm.VStream(vstreamCtx, tabletType, vgtid, nil, nil, func(events []*binlogdatapb.VEvent) error {
				return fmt.Errorf("unexpected events: %v", events)
			})

			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, logger.String(), tc.wantErr)
			} else {
				// Otherwise we simply expect the context to timeout
				require.Error(t, err)
				require.ErrorIs(t, vterrors.UnwrapAll(err), context.DeadlineExceeded)
			}

			logger.Clear()
		})
	}
}

func newTestVStreamManager(ctx context.Context, hc discovery.HealthCheck, serv srvtopo.Server, cell string) *vstreamManager {
	gw := NewTabletGateway(ctx, hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return newVStreamManager(srvResolver, serv, cell)
}

func getVEvents(keyspace, shard string, count, idx int64) []*binlogdatapb.VEvent {
	mu.Lock()
	defer mu.Unlock()
	var vevents []*binlogdatapb.VEvent
	var i int64
	currentTime := time.Now().Unix()
	for i = count; i > 0; i-- {
		j := i + idx
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID, Gtid: fmt.Sprintf("gtid-%s-%d", shard, j),
			Timestamp:   currentTime - j,
			CurrentTime: currentTime * 1e9,
			Keyspace:    keyspace,
			Shard:       shard,
		})

		vevents = append(vevents, &binlogdatapb.VEvent{
			Type:        binlogdatapb.VEventType_COMMIT,
			Timestamp:   currentTime - j,
			CurrentTime: currentTime * 1e9,
			Keyspace:    keyspace,
			Shard:       shard,
		})
	}
	return vevents
}

func getSandboxTopo(ctx context.Context, cell string, keyspace string, shards []string) *sandboxTopo {
	st := newSandboxForCells(ctx, []string{cell})
	ts := st.topoServer
	ts.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{})
	ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{})
	for _, shard := range shards {
		ts.CreateShard(ctx, keyspace, shard)
	}
	return st
}

func getSandboxTopoMultiCell(ctx context.Context, cells []string, keyspace string, shards []string) *sandboxTopo {
	st := newSandboxForCells(ctx, cells)
	ts := st.topoServer

	for _, cell := range cells {
		ts.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{})
	}

	ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{})

	for _, shard := range shards {
		ts.CreateShard(ctx, keyspace, shard)
	}
	return st
}

func addTabletToSandboxTopo(tb testing.TB, ctx context.Context, st *sandboxTopo, ks, shard string, tablet *topodatapb.Tablet) {
	_, err := st.topoServer.UpdateShardFields(ctx, ks, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		return nil
	})
	require.NoError(tb, err)
	err = st.topoServer.CreateTablet(ctx, tablet)
	require.NoError(tb, err)
}
