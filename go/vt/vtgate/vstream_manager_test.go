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
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/discovery"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/gateway"
)

func TestVStream(t *testing.T) {
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
	wantvgtid1 := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "gtid01",
		}},
	}
	want1 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: wantvgtid1},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_GTID, Gtid: "gtid02"},
		{Type: binlogdatapb.VEventType_DDL},
	}
	wantvgtid2 := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "gtid02",
		}},
	}
	want2 := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: wantvgtid2},
		{Type: binlogdatapb.VEventType_DDL},
	}}
	sbc0.AddVStreamEvents(send2, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 1
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
		defer func() { count++ }()

		got := &binlogdatapb.VStreamResponse{Events: events}
		want := want1
		if count == 2 {
			want = want2
		}
		if !proto.Equal(got, want) {
			t.Fatalf("vstream(%d):\n%v, want\n%v", count, got, want)
		}
		if count == 2 {
			cancel()
		}
		return nil
	})
	wantErr := "context canceled"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
	}
}

// TestVStreamChunks ensures that a transaction that's broken
// into chunks is sent together.
func TestVStreamChunks(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	_ = vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got *binlogdatapb.VGtid
	i := 0
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
	_ = vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
		defer func() { i++ }()
		for _, ev := range events {
			if ev.Type == binlogdatapb.VEventType_VGTID {
				got = ev.Vgtid
			}
		}
		if i == 1 {
			cancel()
		}
		return nil
	})
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
	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)

	send1 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(send1, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "aa"))

	send2 := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	sbc0.AddVStreamEvents(send2, nil)
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "bb"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cc"))
	sbc0.AddVStreamEvents(nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "final error"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
		count++
		return nil
	})
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantErr := "final error"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
	}
}

func TestVStreamHeartbeat(t *testing.T) {
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
	wantvgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "gtid01",
		}},
	}
	want := &binlogdatapb.VStreamResponse{Events: []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_VGTID, Vgtid: wantvgtid},
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestVStream.t0"}},
		{Type: binlogdatapb.VEventType_COMMIT},
	}}
	sbc0.AddVStreamEvents(send1, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: name,
			Shard:    "-20",
			Gtid:     "pos",
		}},
	}
	err := vsm.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
		got := &binlogdatapb.VStreamResponse{Events: events}
		if !proto.Equal(got, want) {
			t.Fatalf("vstream:\n%v, want\n%v", got, want)
		}
		cancel()
		return nil
	})
	wantErr := "context canceled"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("vstream end: %v, must contain %v", err.Error(), wantErr)
	}
}

func TestResolveVStreamParams(t *testing.T) {
	name := "TestVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	vsm := newTestVStreamManager(hc, new(sandboxTopo), "aa")
	testcases := []struct {
		input  *binlogdatapb.VGtid
		output *binlogdatapb.VGtid
	}{{
		input: &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "TestVStream",
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
		vgtid, filter, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, tcase.input, nil)
		require.NoError(t, err)
		assert.Equal(t, tcase.output, vgtid)
		assert.Equal(t, wantFilter, filter)
	}
	// Special-case empty vgtid because output is too big.
	vgtid, _, err := vsm.resolveParams(context.Background(), topodatapb.TabletType_REPLICA, nil, nil)
	require.NoError(t, err)
	if got, want := len(vgtid.ShardGtids), 8; want >= got {
		t.Errorf("len(vgtid.ShardGtids): %v, must be >%d", got, want)
	}
}

func newTestVStreamManager(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *vstreamManager {
	gw := gateway.GetCreator()(context.Background(), hc, serv, cell, 3)
	srvResolver := srvtopo.NewResolver(serv, gw, cell)
	return newVStreamManager(srvResolver, serv, cell)
}
