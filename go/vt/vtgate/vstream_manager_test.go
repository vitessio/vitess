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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	"context"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

var mu sync.Mutex

func getVEvents(shard string, count, idx int64) []*binlogdatapb.VEvent {
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
		})

		vevents = append(vevents, &binlogdatapb.VEvent{
			Type:        binlogdatapb.VEventType_COMMIT,
			Timestamp:   currentTime - j,
			CurrentTime: currentTime * 1e9,
		})
	}
	return vevents
}

func TestVStreamSkew(t *testing.T) {
	stream := func(conn *sandboxconn.SandboxConn, shard string, count, idx int64) {
		vevents := getVEvents(shard, count, idx)
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
	vstreamSkewDelayCount = stats.NewCounter("VStreamEventsDelayedBySkewAlignment",
		"Number of events that had to wait because the skew across shards was too high")
	for idx, tcase := range tcases {
		t.Run("", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			name := fmt.Sprintf("TestVStreamSkew-%d", idx)
			_ = createSandbox(name)
			hc := discovery.NewFakeHealthCheck()
			vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
			shard0 := "-20"
			shard1 := "20-40"
			vgtid := &binlogdatapb.VGtid{ShardGtids: []*binlogdatapb.ShardGtid{}}
			want := int64(0)
			var sbc0, sbc1 *sandboxconn.SandboxConn
			if tcase.shard0idx != 0 {
				sbc0 = hc.AddTestTablet("aa", "1.1.1.1", 1001, name, shard0, topodatapb.TabletType_MASTER, true, 1, nil)
				sbc0.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: name, Gtid: "pos", Shard: "-20"})
				go stream(sbc0, shard0, tcase.numEventsPerShard, tcase.shard0idx)
			}
			if tcase.shard1idx != 0 {
				sbc1 = hc.AddTestTablet("aa", "1.1.1.1", 1002, name, shard1, topodatapb.TabletType_MASTER, true, 1, nil)
				sbc1.VStreamCh = make(chan *binlogdatapb.VEvent)
				want += 2 * tcase.numEventsPerShard
				vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: name, Gtid: "pos", Shard: "20-40"})
				go stream(sbc1, shard1, tcase.numEventsPerShard, tcase.shard1idx)
			}
			ch := startVStream(ctx, t, vsm, vgtid, true)
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

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
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
				Keyspace: name,
				Shard:    "-20",
				Gtid:     "gtid02",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send2, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	ch := make(chan *binlogdatapb.VStreamResponse)
	go func() {
		err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
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

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)

	for i := 0; i < 100; i++ {
		sbc0.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_DDL}}, nil)
		sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}}}, nil)
	}
	sbc1.AddVStreamEvents([]*binlogdatapb.VEvent{{Type: binlogdatapb.VEventType_COMMIT}}, nil)

	rowEncountered := false
	doneCounting := false
	rowCount := 0
	ddlCount := 0
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: name,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}
	_ = vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		switch events[0].Type {
		case binlogdatapb.VEventType_ROW:
			if doneCounting {
				t.Errorf("Unexpected event, only expecting DDL: %v", events[0])
				return fmt.Errorf("unexpected event: %v", events[0])
			}
			rowEncountered = true
			rowCount++
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
			ddlCount++
		default:
			t.Errorf("Unexpected event: %v", events[0])
			return fmt.Errorf("unexpected event: %v", events[0])
		}
		if rowCount == 100 && ddlCount == 100 {
			cancel()
		}
		return nil
	})
	if rowCount != 100 {
		t.Errorf("rowCount: %d, want 100", rowCount)
	}
	if ddlCount != 100 {
		t.Errorf("ddlCount: %d, want 100", ddlCount)
	}
}

func TestVStreamMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)

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
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}, {
			Keyspace: name,
			Shard:    "20-40",
			Gtid:     "pos",
		}},
	}
	ch := startVStream(ctx, t, vsm, vgtid, false)
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
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "gtid01",
		}, {
			Keyspace: name,
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

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)

	commit := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(commit, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "aa"))
	sbc0.AddVStreamEvents(commit, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "bb"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cc"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "final error"))

	count := 0
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
		count++
		return nil
	})
	assert.Equal(t, 2, count)
	wantErr := "final error"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
	}
}

func TestVStreamHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)

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
				Keyspace: name,
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
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	ch := startVStream(ctx, t, vsm, vgtid, false)
	verifyEvents(t, ch, want)
}

func TestVStreamJournalOneToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "-10", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, name, "10-20", topodatapb.TabletType_MASTER, true, 1, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
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
				Keyspace: name,
				Shard:    "-10",
				Gtid:     "pos10",
			}, {
				Keyspace: name,
				Shard:    "10-20",
				Gtid:     "pos1020",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: name,
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
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	ch := startVStream(ctx, t, vsm, vgtid, false)
	verifyEvents(t, ch, want1)

	// The following two events from the different shards can come in any order.
	// But the resulting VGTID should be the same after both are received.
	<-ch
	got := <-ch
	wantevent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_VGTID,
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
				Shard:    "-10",
				Gtid:     "gtid03",
			}, {
				Keyspace: name,
				Shard:    "10-20",
				Gtid:     "gtid04",
			}},
		},
	}
	if !proto.Equal(got.Events[0], wantevent) {
		t.Errorf("vgtid: %v, want %v", got.Events[0], wantevent)
	}
}

func TestVStreamJournalManyToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Variable names are maintained like in OneToMany, but order is different.
	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "-10", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, name, "10-20", topodatapb.TabletType_MASTER, true, 1, nil)

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
				Keyspace: name,
				Shard:    "-20",
				Gtid:     "pos20",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: name,
				Shard:    "-10",
			}, {
				Keyspace: name,
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
				Keyspace: name,
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
			Keyspace: name,
			Shard:    "-10",
			Gtid:     "pos10",
		}, {
			Keyspace: name,
			Shard:    "10-20",
			Gtid:     "pos1020",
		}},
	}
	ch := startVStream(ctx, t, vsm, vgtid, false)
	// The following two events from the different shards can come in any order.
	// But the resulting VGTID should be the same after both are received.
	<-ch
	got := <-ch
	wantevent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_VGTID,
		Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
				Shard:    "-10",
				Gtid:     "gtid03",
			}, {
				Keyspace: name,
				Shard:    "10-20",
				Gtid:     "gtid04",
			}},
		},
	}
	if !proto.Equal(got.Events[0], wantevent) {
		t.Errorf("vgtid: %v, want %v", got.Events[0], wantevent)
	}
	verifyEvents(t, ch, want1)
}

func TestVStreamJournalNoMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid01"},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
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
				Keyspace: name,
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
				Keyspace: name,
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
				Keyspace: name,
				Shard:    "c0-",
				Gtid:     "posc0",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: name,
				Shard:    "c0-e0",
			}, {
				Keyspace: name,
				Shard:    "e0-",
			}},
		}},
		{Type: binlogdatapb.VEventType_GTID, Gtid: "jn2"},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	wantjn2 := &binlogdata.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
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
				Keyspace: name,
				Shard:    "-20",
				Gtid:     "gtid03",
			}},
		}},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send3, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	ch := startVStream(ctx, t, vsm, vgtid, false)
	verifyEvents(t, ch, want1, wantjn1, want2, wantjn2, want3)
}

func TestVStreamJournalPartialMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Variable names are maintained like in OneToMany, but order is different.1
	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	_ = hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "-10", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, name, "10-20", topodatapb.TabletType_MASTER, true, 1, nil)

	send := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_JOURNAL, Journal: &binlogdatapb.Journal{
			Id:            1,
			MigrationType: binlogdatapb.MigrationType_SHARDS,
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: name,
				Shard:    "10-30",
				Gtid:     "pos1040",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: name,
				Shard:    "10-20",
			}, {
				Keyspace: name,
				Shard:    "20-30",
			}},
		}},
	}
	sbc2.AddVStreamEvents(send, nil)

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-10",
			Gtid:     "pos10",
		}, {
			Keyspace: name,
			Shard:    "10-20",
			Gtid:     "pos1020",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
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
				Keyspace: name,
				Shard:    "10-30",
				Gtid:     "pos1040",
			}},
			Participants: []*binlogdatapb.KeyspaceShard{{
				Keyspace: name,
				Shard:    "20-30",
			}, {
				Keyspace: name,
				Shard:    "10-20",
			}},
		}},
	}
	sbc2.AddVStreamEvents(send, nil)
	err = vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{}, func(events []*binlogdatapb.VEvent) error {
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
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
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

func newTestVStreamManager(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *vstreamManager {
	gw := NewTabletGateway(context.Background(), hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return newVStreamManager(srvResolver, serv, cell)
}

func startVStream(ctx context.Context, t *testing.T, vsm *vstreamManager, vgtid *binlogdatapb.VGtid, minimizeSkew bool) <-chan *binlogdatapb.VStreamResponse {
	ch := make(chan *binlogdatapb.VStreamResponse)
	go func() {
		_ = vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, &vtgatepb.VStreamFlags{MinimizeSkew: true}, func(events []*binlogdatapb.VEvent) error {
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
		for _, event := range got.Events {
			event.Timestamp = 0
		}
		if !proto.Equal(got, want) {
			t.Errorf("vstream(%d):\n%v, want\n%v", i, got, want)
		}
	}
}
