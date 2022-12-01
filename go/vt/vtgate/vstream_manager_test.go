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
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/sync2"

	"vitess.io/vitess/go/vt/topo"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/srvtopo"
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
			vsm := newTestVStreamManager(hc, st, cell)
			vgtid := &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{}}
			want := int64(0)
			var sbc0, sbc1 *sandboxconn.SandboxConn
			if tcase.shard0idx != 0 {
				sbc0 = hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
				addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
				sbc0.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: ks, Gtid: "pos", Shard: "-20"})
				go stream(sbc0, ks, "-20", tcase.numEventsPerShard, tcase.shard0idx)
			}
			if tcase.shard1idx != 0 {
				sbc1 = hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
				addTabletToSandboxTopo(t, st, ks, "20-40", sbc1.Tablet())
				sbc1.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: ks, Gtid: "pos", Shard: "20-40"})
				go stream(sbc1, ks, "20-40", tcase.numEventsPerShard, tcase.shard1idx)
			}
			ch := startVStream(ctx, t, vsm, vgtid, &vtgatepb.VStreamFlags{MinimizeSkew: true})
			var receivedEvents []*binlogdatapb.VEvent
			for len(receivedEvents) < int(want) {
				select {
				case <-time.After(1 * time.Minute):
					require.FailNow(t, "test timed out")
				case response := <-ch:
					receivedEvents = append(receivedEvents, response.Events...)
				}
			}
			require.Equal(t, int(want), int(len(receivedEvents)))
			require.Equal(t, tcase.expectedDelays, vsm.GetTotalStreamDelay()-previousDelays)
			previousDelays = vsm.GetTotalStreamDelay()
		})
	}
}

func TestVStreamEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})

	vsm := newTestVStreamManager(hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())

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
	ch := make(chan *binlogdatapb.VStreamResponse)
	go func() {
		err := vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
			ch <- &binlogdatapb.VStreamResponse{Events: events}
			return nil
		})
		wantErr := "context canceled"
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
		}
		ch <- nil
	}()
	verifyEvents(t, ch, want1, want2)

	// Ensure the go func error return was verified.
	cancel()
	<-ch
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
	vsm := newTestVStreamManager(hc, st, cell)
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "20-40", sbc1.Tablet())

	for i := 0; i < 100; i++ {
		sbc0.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_DDL}}, nil)
		sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}}}, nil)
	}
	sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_COMMIT}}, nil)

	rowEncountered := false
	doneCounting := false
	var rowCount, ddlCount sync2.AtomicInt32
	rowCount.Set(0)
	ddlCount.Set(0)
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
	_ = vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		switch events[0].Type {
		case binlogdatapb.VEventType_ROW:
			if doneCounting {
				t.Errorf("Unexpected event, only expecting DDL: %v", events[0])
				return fmt.Errorf("unexpected event: %v", events[0])
			}
			rowEncountered = true
			rowCount.Add(1)
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
			ddlCount.Add(1)
		default:
			t.Errorf("Unexpected event: %v", events[0])
			return fmt.Errorf("unexpected event: %v", events[0])
		}
		if rowCount.Get() == int32(100) && ddlCount.Get() == int32(100) {
			cancel()
		}
		return nil
	})
	assert.Equal(t, int32(100), rowCount.Get())
	assert.Equal(t, int32(100), ddlCount.Get())
}

func TestVStreamManagerGetCells(t *testing.T) {
	type testArgs struct {
		name        string
		optCells    string
		cellAlias   bool
		resultCells []string
	}

	tcases := []*testArgs{
		{"default-local", "", false, []string{"aa"}},
		{"default-local-cell-alias", "", true, []string{"local:aa", "region1"}},
		{"with-opt-cells", "local,bb,cc", true, []string{"local:aa", "bb", "cc"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(hc, st, "aa")
	ts, _ := st.GetTopoServer()

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs := &vstream{
				vgtid:              nil,
				tabletType:         topodatapb.TabletType_PRIMARY,
				optCells:           tcase.optCells,
				filter:             nil,
				send:               nil,
				resolver:           nil,
				journaler:          nil,
				minimizeSkew:       false,
				stopOnReshard:      true,
				skewTimeoutSeconds: 0,
				timestamps:         nil,
				vsm:                vsm,
				eventCh:            nil,
				heartbeatInterval:  0,
				ts:                 ts,
			}

			if tcase.cellAlias {
				cellsAlias := &topodatapb.CellsAlias{
					Cells: []string{"aa", "bb"},
				}
				assert.Nil(t, ts.CreateCellsAlias(context.Background(), "region1", cellsAlias), "failed to create cell alias")
				defer cleanupGetCellTests(t, ts, "region1")
			}

			got := vs.getCells(ctx)
			assert.Equal(t, len(got), len(tcase.resultCells))
			assert.Equal(t, got, tcase.resultCells)
		})
	}
}

func cleanupGetCellTests(t *testing.T, ts *topo.Server, region string) {
	if region != "" {
		if err := ts.DeleteCellsAlias(context.Background(), region); err != nil {
			t.Logf("DeleteCellsAlias(%s) failed: %v", region, err)
		}
	}
}

func TestVStreamMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "20-40"})
	vsm := newTestVStreamManager(hc, st, "aa")
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "20-40", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "20-40", sbc1.Tablet())

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
	ch := startVStream(ctx, t, vsm, vgtid, nil)
	<-ch
	response := <-ch
	var got *binlogdatapb.VGtid
	for _, ev := range response.Events {
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
	if !proto.Equal(got, want) {
		t.Errorf("VGtid:\n%v, want\n%v", got, want)
	}
}

func TestVStreamRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)

	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(hc, st, "aa")
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
	commit := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(commit, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "aa"))
	sbc0.AddVStreamEvents(commit, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "bb"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cc"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "final error"))
	var count sync2.AtomicInt32
	count.Set(0)
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: ks,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		count.Add(1)
		return nil
	})
	wantErr := "final error"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
	}
	time.Sleep(100 * time.Millisecond) // wait for goroutine within VStream to finish
	assert.Equal(t, int32(2), count.Get())
}

func TestVStreamShouldNotSendSourceHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())

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
	ch := startVStream(ctx, t, vsm, vgtid, nil)
	verifyEvents(t, ch, want)
}

func TestVStreamJournalOneToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "-10", "10-20"})
	vsm := newTestVStreamManager(hc, st, "aa")
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet(cell, "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "10-20", sbc2.Tablet())

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
	ch := startVStream(ctx, t, vsm, vgtid, nil)
	verifyEvents(t, ch, want1)

	// The following two events from the different shards can come in any order.
	// But the resulting VGTID should be the same after both are received.
	<-ch
	got := <-ch
	wantevent := &binlogdatapb.VEvent{
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
	}
	gotEvent := got.Events[0]
	gotEvent.Keyspace = ""
	gotEvent.Shard = ""
	if !proto.Equal(gotEvent, wantevent) {
		t.Errorf("vgtid: %v, want %v", got.Events[0], wantevent)
	}
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
	vsm := newTestVStreamManager(hc, st, cell)
	sbc0 := hc.AddTestTablet(cell, "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
	sbc1 := hc.AddTestTablet(cell, "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet(cell, "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "10-20", sbc2.Tablet())

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
	ch := startVStream(ctx, t, vsm, vgtid, nil)
	// The following two events from the different shards can come in any order.
	// But the resulting VGTID should be the same after both are received.
	<-ch
	got := <-ch
	wantevent := &binlogdatapb.VEvent{
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
	}
	gotEvent := got.Events[0]
	gotEvent.Keyspace = ""
	gotEvent.Shard = ""
	if !proto.Equal(gotEvent, wantevent) {
		t.Errorf("vgtid: %v, want %v", got.Events[0], wantevent)
	}
	verifyEvents(t, ch, want1)
}

func TestVStreamJournalNoMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(hc, st, "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())

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
	wantjn1 := &binlogdata.VStreamResponse{Events: []*binlogdatapb.VEvent{
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
	wantjn2 := &binlogdata.VStreamResponse{Events: []*binlogdatapb.VEvent{
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
	ch := startVStream(ctx, t, vsm, vgtid, nil)
	verifyEvents(t, ch, want1, wantjn1, want2, wantjn2, want3)
}

func TestVStreamJournalPartialMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Variable names are maintained like in OneToMany, but order is different.1
	ks := "TestVStream"
	cell := "aa"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20", "-10", "10-20"})
	vsm := newTestVStreamManager(hc, st, "aa")
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, ks, "-10", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-10", sbc1.Tablet())
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, ks, "10-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "10-20", sbc2.Tablet())

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
		t.Errorf("unexpected events: %v", events)
		return nil
	})
	wantErr := "not all journaling participants are in the stream"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err, wantErr)
	}

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
		t.Errorf("unexpected events: %v", events)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err, wantErr)
	}
	cancel()
}

func TestResolveVStreamParams(t *testing.T) {
	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck(nil)
	vsm := newTestVStreamManager(hc, newSandboxForCells([]string{"aa"}), "aa")
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
			}},
		},
		err: "if shards are unspecified, the Gtid value must be 'current'",
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
	// Special-case: empty keyspace because output is too big.
	input := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Gtid: "current",
		}},
	}
	vgtid, _, _, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, input, nil, nil)
	require.NoError(t, err, input)
	if got, want := len(vgtid.ShardGtids), 8; want >= got {
		t.Errorf("len(vgtid.ShardGtids): %v, must be >%d", got, want)
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
	cell := "aa"
	ks := "TestVStream"
	_ = createSandbox(ks)
	hc := discovery.NewFakeHealthCheck(nil)
	st := getSandboxTopo(ctx, cell, ks, []string{"-20"})
	vsm := newTestVStreamManager(hc, st, cell)
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, ks, "-20", topodatapb.TabletType_PRIMARY, true, 1, nil)
	addTabletToSandboxTopo(t, st, ks, "-20", sbc0.Tablet())
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
			var mu sync.Mutex
			var heartbeatCount int
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, &vtgatepb.VStreamFlags{HeartbeatInterval: tcase.heartbeatInterval},
					func(events []*binlogdatapb.VEvent) error {
						mu.Lock()
						defer mu.Unlock()
						for _, event := range events {
							if event.Type == binlogdatapb.VEventType_HEARTBEAT {
								heartbeatCount++
							}
						}
						return nil
					})
			}()
			time.Sleep(time.Duration(4500) * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			require.Equalf(t, heartbeatCount, tcase.want, "got %d, want %d", heartbeatCount, tcase.want)
			cancel()
		})
	}
}

func newTestVStreamManager(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *vstreamManager {
	gw := NewTabletGateway(context.Background(), hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return newVStreamManager(srvResolver, serv, cell)
}

func startVStream(ctx context.Context, t *testing.T, vsm *vstreamManager, vgtid *binlogdatapb.VGtid, flags *vtgatepb.VStreamFlags) <-chan *binlogdatapb.VStreamResponse {
	if flags == nil {
		flags = &vtgatepb.VStreamFlags{}
	}
	ch := make(chan *binlogdatapb.VStreamResponse)
	go func() {
		_ = vsm.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, nil, flags, func(events []*binlogdatapb.VEvent) error {
			ch <- &binlogdatapb.VStreamResponse{Events: events}
			return nil
		})
	}()
	return ch
}

func verifyEvents(t *testing.T, ch <-chan *binlogdatapb.VStreamResponse, wants ...*binlogdatapb.VStreamResponse) {
	t.Helper()
	for i, want := range wants {
		got := <-ch
		require.NotNil(t, got)
		for _, event := range got.Events {
			event.Timestamp = 0
		}
		if !proto.Equal(got, want) {
			t.Errorf("vstream(%d):\n%v, want\n%v", i, got, want)
		}
	}
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
	st := newSandboxForCells([]string{cell})
	ts := st.topoServer
	ts.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{})
	ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{})
	for _, shard := range shards {
		ts.CreateShard(ctx, keyspace, shard)
	}
	return st
}

func addTabletToSandboxTopo(t *testing.T, st *sandboxTopo, ks, shard string, tablet *topodatapb.Tablet) {
	_, err := st.topoServer.UpdateShardFields(ctx, ks, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = tablet.Alias
		return nil
	})
	require.NoError(t, err)
	err = st.topoServer.CreateTablet(ctx, tablet)
	require.NoError(t, err)
}
