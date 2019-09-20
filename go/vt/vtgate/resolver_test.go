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

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

func TestResolverExecuteKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyspaceIds", func(res *Resolver) (*sqltypes.Result, error) {
		return res.Execute(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyspaceIds",
			topodatapb.TabletType_MASTER,
			key.DestinationKeyspaceIDs([][]byte{{0x10}, {0x25}}),
			nil,
			false,
			nil,
			nil)
	})
}

func TestResolverExecuteKeyRanges(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyRanges", func(res *Resolver) (*sqltypes.Result, error) {
		return res.Execute(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyRanges",
			topodatapb.TabletType_MASTER,
			key.DestinationKeyRanges([]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x25}}}),
			nil,
			false,
			nil,
			nil)
	})
}

func TestResolverExecuteEntityIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteEntityIds", func(res *Resolver) (*sqltypes.Result, error) {
		return res.ExecuteEntityIds(context.Background(),
			"query",
			nil,
			"TestResolverExecuteEntityIds",
			"col",
			[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
				{
					Type:       sqltypes.Int64,
					Value:      []byte("0"),
					KeyspaceId: []byte{0x10},
				},
				{
					Type:       sqltypes.VarBinary,
					Value:      []byte("1"),
					KeyspaceId: []byte{0x25},
				},
			},
			topodatapb.TabletType_MASTER,
			nil,
			false,
			nil)
	})
}

func TestResolverExecuteBatchKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteBatchKeyspaceIds", func(res *Resolver) (*sqltypes.Result, error) {
		queries := []*vtgatepb.BoundKeyspaceIdQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: "TestResolverExecuteBatchKeyspaceIds",
			KeyspaceIds: [][]byte{
				{0x10},
				{0x25},
			},
		}}
		qrs, err := res.ExecuteBatch(context.Background(),
			topodatapb.TabletType_MASTER,
			false,
			nil,
			nil,
			func() (*scatterBatchRequest, error) {
				return boundKeyspaceIDQueriesToScatterBatchRequest(context.Background(), res.resolver, queries, topodatapb.TabletType_MASTER)
			})
		if err != nil {
			return nil, err
		}
		return &qrs[0], err
	})
}

func TestResolverStreamExecuteKeyspaceIds(t *testing.T) {
	keyspace := "TestResolverStreamExecuteKeyspaceIds"
	testResolverStreamGeneric(t, keyspace, func(res *Resolver) (*sqltypes.Result, error) {
		qr := new(sqltypes.Result)
		err := res.StreamExecute(context.Background(),
			"query",
			nil,
			keyspace,
			topodatapb.TabletType_MASTER,
			key.DestinationKeyspaceIDs([][]byte{{0x10}, {0x15}}),
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
	testResolverStreamGeneric(t, keyspace, func(res *Resolver) (*sqltypes.Result, error) {
		qr := new(sqltypes.Result)
		err := res.StreamExecute(context.Background(),
			"query",
			nil,
			keyspace,
			topodatapb.TabletType_MASTER,
			key.DestinationKeyspaceIDs([][]byte{{0x10}, {0x15}, {0x25}}),
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
}

func TestResolverStreamExecuteKeyRanges(t *testing.T) {
	keyspace := "TestResolverStreamExecuteKeyRanges"
	// streaming a single shard
	testResolverStreamGeneric(t, keyspace, func(res *Resolver) (*sqltypes.Result, error) {
		qr := new(sqltypes.Result)
		err := res.StreamExecute(context.Background(),
			"query",
			nil,
			keyspace,
			topodatapb.TabletType_MASTER,
			key.DestinationKeyRanges([]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x15}}}),
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
	// streaming multiple shards
	testResolverStreamGeneric(t, keyspace, func(res *Resolver) (*sqltypes.Result, error) {
		qr := new(sqltypes.Result)
		err := res.StreamExecute(context.Background(),
			"query",
			nil,
			keyspace,
			topodatapb.TabletType_MASTER,
			key.DestinationKeyRanges([]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x25}}}),
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
}

func testResolverGeneric(t *testing.T, name string, action func(res *Resolver) (*sqltypes.Result, error)) {
	t.Run("successful execute", func(t *testing.T) {
		createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)

		_, err := action(res)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
	})

	t.Run("non-retryable failure", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		sbc0 := hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
		sbc1.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1
		_, err := action(res)
		want := []string{
			fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20): INVALID_ARGUMENT error", name),
			fmt.Sprintf("target: %s.20-40.master, used tablet: aa-0 (20-40): INTERNAL error", name),
		}
		if err == nil {
			t.Errorf("want\n%v\ngot\n%v", want, err)
		} else {
			got := strings.Split(err.Error(), "\n")
			sort.Strings(got)
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want\n%v\ngot\n%v", want, got)
			}
		}
		// Ensure that we tried only once
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried topo only once when mapping KeyspaceId/KeyRange to shards
		if s.SrvKeyspaceCounter != 1 {
			t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
		}

	})

	t.Run("retryable failure, no sharding event", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		sbc0 := hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc0.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
		sbc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
		_, err := action(res)
		want := []string{
			fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20): FAILED_PRECONDITION error", name),
			fmt.Sprintf("target: %s.20-40.master, used tablet: aa-0 (20-40): FAILED_PRECONDITION error", name),
		}
		if err == nil {
			t.Errorf("want\n%v\ngot\n%v", want, err)
		} else {
			got := strings.Split(err.Error(), "\n")
			sort.Strings(got)
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want\n%v\ngot\n%v", want, got)
			}
		}
		// Ensure that we tried only once.
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried topo only twice.
		if s.SrvKeyspaceCounter != 2 {
			t.Errorf("want 2, got %v", s.SrvKeyspaceCounter)
		}
	})

	t.Run("no failure, initial vertical resharding", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		addSandboxServedFrom(name, name+"ServedFrom0")
		sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		s0 := createSandbox(name + "ServedFrom0") // make sure we have a fresh copy
		s0.ShardSpec = "-80-"
		sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, name+"ServedFrom0", "-80", topodatapb.TabletType_MASTER, true, 1, nil)
		_, err := action(res)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure original keyspace is not used.
		if execCount := sbc0.ExecCount.Get(); execCount != 0 {
			t.Errorf("want 0, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 0 {
			t.Errorf("want 0, got %v", execCount)
		}
		// Ensure redirected keyspace is accessed once.
		if execCount := sbc2.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried each keyspace only once.
		if s.SrvKeyspaceCounter != 1 {
			t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
		}
		if s0.SrvKeyspaceCounter != 1 {
			t.Errorf("want 1, got %v", s0.SrvKeyspaceCounter)
		}
		s0.Reset()
	})

	//
	t.Run("retryable failure, vertical resharding", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")

		sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
		i := 0
		s.SrvKeyspaceCallback = func() {
			if i == 1 {
				addSandboxServedFrom(name, name+"ServedFrom")
				hc.Reset()
				hc.AddTestTablet("aa", "1.1.1.1", 1001, name+"ServedFrom", "-20", topodatapb.TabletType_MASTER, true, 1, nil)
				hc.AddTestTablet("aa", "1.1.1.1", 1002, name+"ServedFrom", "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
			}
			i++
		}
		_, err := action(res)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure that we tried only once on the original conn.
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried topo only 3 times.
		if s.SrvKeyspaceCounter != 3 {
			t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
		}
	})

	t.Run("retryable failure, horizontal resharding", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")

		s.Reset()
		hc.Reset()
		sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
		i := 0
		s.SrvKeyspaceCallback = func() {
			if i == 1 {
				s.ShardSpec = "-20-30-40-60-80-a0-c0-e0-"
				hc.Reset()
				hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
				hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-30", topodatapb.TabletType_MASTER, true, 1, nil)
			}
			i++
		}
		_, err := action(res)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		// Ensure that we tried only once on original guy.
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		if execCount := sbc1.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried topo only twice.
		if s.SrvKeyspaceCounter != 2 {
			t.Errorf("want 2, got %v", s.SrvKeyspaceCounter)
		}
	})
}

func testResolverStreamGeneric(t *testing.T, name string, action func(res *Resolver) (*sqltypes.Result, error)) {
	t.Run("successful execute", func(t *testing.T) {
		createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		_, err := action(res)
		if err != nil {
			t.Errorf("want nil, got %v", err)
		}
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}

	})

	t.Run("failure", func(t *testing.T) {
		s := createSandbox(name)
		hc := discovery.NewFakeHealthCheck()
		res := newTestResolver(hc, new(sandboxTopo), "aa")
		sbc0 := hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
		hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
		sbc0.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1
		_, err := action(res)
		want := fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20): INTERNAL error", name)
		if err == nil || err.Error() != want {
			t.Errorf("want\n%s\ngot\n%v", want, err)
		}
		// Ensure that we tried only once.
		if execCount := sbc0.ExecCount.Get(); execCount != 1 {
			t.Errorf("want 1, got %v", execCount)
		}
		// Ensure that we tried topo only once
		if s.SrvKeyspaceCounter != 1 {
			t.Errorf("want 1, got %v", s.SrvKeyspaceCounter)
		}
	})
}

func TestResolverMessageAckSharded(t *testing.T) {
	name := "TestResolverMessageAckSharded"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 := hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)

	sbc0.MessageIDs = nil
	sbc1.MessageIDs = nil
	idKeyspaceIDs := []*vtgatepb.IdKeyspaceId{
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("1"),
			},
			KeyspaceId: []byte{0x10},
		},
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("3"),
			},
			KeyspaceId: []byte{0x30},
		},
	}
	count, err := res.MessageAckKeyspaceIds(context.Background(), name, "user", idKeyspaceIDs)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}}
	if !sqltypes.Proto3ValuesEqual(sbc0.MessageIDs, wantids) {
		t.Errorf("sbc0.MessageIDs: %+v, want %+v\n", sbc0.MessageIDs, wantids)
	}
	wantids = []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	if !sqltypes.Proto3ValuesEqual(sbc1.MessageIDs, wantids) {
		t.Errorf("sbc1.MessageIDs: %+v, want %+v\n", sbc1.MessageIDs, wantids)
	}
}

func TestResolverMessageAckUnsharded(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
	sbc0 := hc.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	sbc0.MessageIDs = nil
	idKeyspaceIDs := []*vtgatepb.IdKeyspaceId{
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("1"),
			},
		},
		{
			Id: &querypb.Value{
				Type:  sqltypes.VarChar,
				Value: []byte("3"),
			},
		},
	}
	count, err := res.MessageAckKeyspaceIds(context.Background(), KsTestUnsharded, "user", idKeyspaceIDs)
	if err != nil {
		t.Error(err)
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	wantids := []*querypb.Value{{
		Type:  sqltypes.VarChar,
		Value: []byte("1"),
	}, {
		Type:  sqltypes.VarChar,
		Value: []byte("3"),
	}}
	if !sqltypes.Proto3ValuesEqual(sbc0.MessageIDs, wantids) {
		t.Errorf("sbc0.MessageIDs: %+v, want %+v\n", sbc0.MessageIDs, wantids)
	}
}

func TestResolverVStream(t *testing.T) {
	name := "TestResolverVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
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
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestResolverVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestResolverVStream.t0"}},
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
	err := res.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

// TestResolverVStreamChunks ensures that a transaction that's broken
// into chunks is sent together.
func TestResolverVStreamChunks(t *testing.T) {
	name := "TestResolverVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
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
	_ = res.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

func TestResolverVStreamMulti(t *testing.T) {
	name := "TestResolverVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
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
	_ = res.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

func TestResolverVStreamRetry(t *testing.T) {
	name := "TestResolverVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
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
	err := res.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

func TestResolverVStreamHeartbeat(t *testing.T) {
	name := "TestResolverVStream"
	_ = createSandbox(name)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
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
		{Type: binlogdatapb.VEventType_FIELD, FieldEvent: &binlogdatapb.FieldEvent{TableName: "TestResolverVStream.f0"}},
		{Type: binlogdatapb.VEventType_ROW, RowEvent: &binlogdatapb.RowEvent{TableName: "TestResolverVStream.t0"}},
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
	err := res.VStream(ctx, topodatapb.TabletType_MASTER, vgtid, nil, func(events []*binlogdatapb.VEvent) error {
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

func TestResolverInsertSqlClause(t *testing.T) {
	clause := "col in (:col1, :col2)"
	tests := [][]string{
		{
			"select a from table",
			"select a from table where " + clause},
		{
			"select a from table where id = 1",
			"select a from table where id = 1 and " + clause},
		{
			"select a from table group by a",
			"select a from table where " + clause + " group by a"},
		{
			"select a from table where id = 1 order by a limit 10",
			"select a from table where id = 1 and " + clause + " order by a limit 10"},
		{
			"select a from table where id = 1 for update",
			"select a from table where id = 1 and " + clause + " for update"},
	}
	for _, test := range tests {
		got := insertSQLClause(test[0], clause)
		if got != test[1] {
			t.Errorf("want '%v', got '%v'", test[1], got)
		}
	}
}

func TestResolverBuildEntityIds(t *testing.T) {
	values := [][]*querypb.Value{
		{
			{
				Type:  querypb.Type_VARCHAR,
				Value: []byte("0"),
			},
			{
				Type:  querypb.Type_INT64,
				Value: []byte("1"),
			},
		},
		{
			{
				Type:  querypb.Type_VARCHAR,
				Value: []byte("2"),
			},
		},
	}
	sql := "select a from table where id=:id"
	entityColName := "uid"
	bindVar := map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(10),
	}
	sqls, bindVars := buildEntityIds(values, sql, entityColName, bindVar)
	wantSqls := []string{
		"select a from table where id=:id and uid in ::uid_entity_ids",
		"select a from table where id=:id and uid in ::uid_entity_ids",
	}
	wantBindVars := []map[string]*querypb.BindVariable{
		{"id": sqltypes.Int64BindVariable(10), "uid_entity_ids": sqltypes.TestBindVariable([]interface{}{"0", 1})},
		{"id": sqltypes.Int64BindVariable(10), "uid_entity_ids": sqltypes.TestBindVariable([]interface{}{"2"})},
	}
	if !reflect.DeepEqual(wantSqls, sqls) {
		t.Errorf("want %+v, got %+v", wantSqls, sqls)
	}
	if !reflect.DeepEqual(wantBindVars, bindVars) {
		t.Errorf("want\n%+v, got\n%+v", wantBindVars, bindVars)
	}
}

func TestResolverExecBatchReresolve(t *testing.T) {
	keyspace := "TestResolverExecBatchReresolve"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")

	sbc := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 20

	callcount := 0
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		callcount++
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: keyspace,
			Shards:   []string{"0"},
		}}
		return boundShardQueriesToScatterBatchRequest(context.Background(), res.resolver, queries, topodatapb.TabletType_MASTER)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, false, nil, nil, buildBatchRequest)
	want := "target: TestResolverExecBatchReresolve.0.master, used tablet: aa-0 (0): FAILED_PRECONDITION error"
	if err == nil || err.Error() != want {
		t.Errorf("want %s, got %v", want, err)
	}
	// Ensure scatter tried a re-resolve
	if callcount != 2 {
		t.Errorf("want 2, got %v", callcount)
	}
	if count := sbc.AsTransactionCount.Get(); count != 0 {
		t.Errorf("want 0, got %v", count)
	}
}

func TestResolverExecBatchAsTransaction(t *testing.T) {
	keyspace := "TestResolverExecBatchAsTransaction"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")

	sbc := hc.AddTestTablet("aa", "0", 1, keyspace, "0", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc.MustFailCodes[vtrpcpb.Code_INTERNAL] = 20

	callcount := 0
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		callcount++
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: keyspace,
			Shards:   []string{"0"},
		}}
		return boundShardQueriesToScatterBatchRequest(context.Background(), res.resolver, queries, topodatapb.TabletType_MASTER)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, true, nil, nil, buildBatchRequest)
	want := "target: TestResolverExecBatchAsTransaction.0.master, used tablet: aa-0 (0): INTERNAL error"
	if err == nil || err.Error() != want {
		t.Errorf("want %v, got %v", want, err)
	}
	// Ensure scatter did not re-resolve
	if callcount != 1 {
		t.Errorf("want 1, got %v", callcount)
	}
	if count := sbc.AsTransactionCount.Get(); count != 1 {
		t.Errorf("want 1, got %v", count)
		return
	}
}

func newTestResolver(hc discovery.HealthCheck, serv srvtopo.Server, cell string) *Resolver {
	sc := newTestScatterConn(hc, serv, cell)
	srvResolver := srvtopo.NewResolver(serv, sc.gateway, cell)
	return NewResolver(srvResolver, serv, cell, sc)
}
