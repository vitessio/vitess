/*
Copyright 2017 Google Inc.

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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// This file uses the sandbox_test framework.

func TestResolverExecuteKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyspaceIds", func(res *Resolver) (*sqltypes.Result, error) {
		return res.ExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyspaceIds",
			[][]byte{{0x10}, {0x25}},
			topodatapb.TabletType_MASTER,
			nil,
			false,
			nil)
	})
}

func TestResolverExecuteKeyRanges(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyRanges", func(res *Resolver) (*sqltypes.Result, error) {
		return res.ExecuteKeyRanges(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyRanges",
			[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x25}}},
			topodatapb.TabletType_MASTER,
			nil,
			false,
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
		qrs, err := res.ExecuteBatchKeyspaceIds(context.Background(),
			[]*vtgatepb.BoundKeyspaceIdQuery{{
				Query: &querypb.BoundQuery{
					Sql:           "query",
					BindVariables: nil,
				},
				Keyspace: "TestResolverExecuteBatchKeyspaceIds",
				KeyspaceIds: [][]byte{
					{0x10},
					{0x25},
				},
			}},
			topodatapb.TabletType_MASTER,
			false,
			nil,
			nil)
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
		err := res.StreamExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			keyspace,
			[][]byte{{0x10}, {0x15}},
			topodatapb.TabletType_MASTER,
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
	testResolverStreamGeneric(t, keyspace, func(res *Resolver) (*sqltypes.Result, error) {
		qr := new(sqltypes.Result)
		err := res.StreamExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			keyspace,
			[][]byte{{0x10}, {0x15}, {0x25}},
			topodatapb.TabletType_MASTER,
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
		err := res.StreamExecuteKeyRanges(context.Background(),
			"query",
			nil,
			keyspace,
			[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x15}}},
			topodatapb.TabletType_MASTER,
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
		err := res.StreamExecuteKeyRanges(context.Background(),
			"query",
			nil,
			keyspace,
			[]*topodatapb.KeyRange{{Start: []byte{0x10}, End: []byte{0x25}}},
			topodatapb.TabletType_MASTER,
			nil,
			func(r *sqltypes.Result) error {
				qr.AppendResult(r)
				return nil
			})
		return qr, err
	})
}

func testResolverGeneric(t *testing.T, name string, action func(res *Resolver) (*sqltypes.Result, error)) {
	// successful execute
	s := createSandbox(name)
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

	// non-retryable failure
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INVALID_ARGUMENT] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1
	_, err = action(res)
	want1 := fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20), INVALID_ARGUMENT error", name)
	want2 := fmt.Sprintf("target: %s.20-40.master, used tablet: aa-0 (20-40), INTERNAL error", name)
	want := []string{want1, want2}
	sort.Strings(want)
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

	// retryable failure, no sharding event
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	sbc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	_, err = action(res)
	want1 = fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20), FAILED_PRECONDITION error", name)
	want2 = fmt.Sprintf("target: %s.20-40.master, used tablet: aa-0 (20-40), FAILED_PRECONDITION error", name)
	want = []string{want1, want2}
	sort.Strings(want)
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

	// no failure, initial vertical resharding
	s.Reset()
	addSandboxServedFrom(name, name+"ServedFrom0")
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	s0 := createSandbox(name + "ServedFrom0") // make sure we have a fresh copy
	s0.ShardSpec = "-80-"
	sbc2 := hc.AddTestTablet("aa", "1.1.1.1", 1003, name+"ServedFrom0", "-80", topodatapb.TabletType_MASTER, true, 1, nil)
	_, err = action(res)
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

	// retryable failure, vertical resharding
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
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
	_, err = action(res)
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

	// retryable failure, horizontal resharding
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1 = hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc1.MustFailCodes[vtrpcpb.Code_FAILED_PRECONDITION] = 1
	i = 0
	s.SrvKeyspaceCallback = func() {
		if i == 1 {
			s.ShardSpec = "-20-30-40-60-80-a0-c0-e0-"
			hc.Reset()
			hc.AddTestTablet("aa", "1.1.1.1", 1001, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
			hc.AddTestTablet("aa", "1.1.1.1", 1002, name, "20-30", topodatapb.TabletType_MASTER, true, 1, nil)
		}
		i++
	}
	_, err = action(res)
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
}

func testResolverStreamGeneric(t *testing.T, name string, action func(res *Resolver) (*sqltypes.Result, error)) {
	// successful execute
	s := createSandbox(name)
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

	// failure
	s.Reset()
	hc.Reset()
	sbc0 = hc.AddTestTablet("aa", "-20", 1, name, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	hc.AddTestTablet("aa", "20-40", 1, name, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)
	sbc0.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1
	_, err = action(res)
	want := fmt.Sprintf("target: %s.-20.master, used tablet: aa-0 (-20), INTERNAL error", name)
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
	shardMap := map[string][]*querypb.Value{
		"-20": {{
			Type:  querypb.Type_VARCHAR,
			Value: []byte("0"),
		}, {
			Type:  querypb.Type_INT64,
			Value: []byte("1"),
		}},
		"20-40": {{
			Type:  querypb.Type_VARCHAR,
			Value: []byte("2"),
		}},
	}
	sql := "select a from table where id=:id"
	entityColName := "uid"
	bindVar := map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(10),
	}
	shards, sqls, bindVars := buildEntityIds(shardMap, sql, entityColName, bindVar)
	wantShards := []string{"-20", "20-40"}
	wantSqls := map[string]string{
		"-20":   "select a from table where id=:id and uid in ::uid_entity_ids",
		"20-40": "select a from table where id=:id and uid in ::uid_entity_ids",
	}
	wantBindVars := map[string]map[string]*querypb.BindVariable{
		"-20":   {"id": sqltypes.Int64BindVariable(10), "uid_entity_ids": sqltypes.TestBindVariable([]interface{}{"0", 1})},
		"20-40": {"id": sqltypes.Int64BindVariable(10), "uid_entity_ids": sqltypes.TestBindVariable([]interface{}{"2"})},
	}
	sort.Strings(wantShards)
	sort.Strings(shards)
	if !reflect.DeepEqual(wantShards, shards) {
		t.Errorf("want %+v, got %+v", wantShards, shards)
	}
	if !reflect.DeepEqual(wantSqls, sqls) {
		t.Errorf("want %+v, got %+v", wantSqls, sqls)
	}
	if !reflect.DeepEqual(wantBindVars, bindVars) {
		t.Errorf("want\n%+v, got\n%+v", wantBindVars, bindVars)
	}
}

func TestResolverDmlOnMultipleKeyspaceIds(t *testing.T) {
	keyspace := "TestResolverDmlOnMultipleKeyspaceIds"
	createSandbox(keyspace)
	hc := discovery.NewFakeHealthCheck()
	res := newTestResolver(hc, new(sandboxTopo), "aa")
	hc.AddTestTablet("aa", "1.1.1.1", 1001, keyspace, "-20", topodatapb.TabletType_MASTER, true, 1, nil)
	hc.AddTestTablet("aa", "1.1.1.1", 1002, keyspace, "20-40", topodatapb.TabletType_MASTER, true, 1, nil)

	errStr := "DML should not span multiple keyspace_ids"
	_, err := res.ExecuteKeyspaceIds(context.Background(),
		"update table set a = b",
		nil,
		keyspace,
		[][]byte{{0x10}, {0x25}},
		topodatapb.TabletType_MASTER,
		nil,
		false,
		nil)
	if err == nil {
		t.Errorf("want %v, got nil", errStr)
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
		return boundShardQueriesToScatterBatchRequest(queries)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, false, nil, nil, buildBatchRequest)
	want := "target: TestResolverExecBatchReresolve.0.master, used tablet: aa-0 (0), FAILED_PRECONDITION error"
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
		return boundShardQueriesToScatterBatchRequest(queries)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, true, nil, nil, buildBatchRequest)
	want := "target: TestResolverExecBatchAsTransaction.0.master, used tablet: aa-0 (0), INTERNAL error"
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

func newTestResolver(hc discovery.HealthCheck, serv topo.SrvTopoServer, cell string) *Resolver {
	sc := newTestScatterConn(hc, serv, cell)
	return NewResolver(serv, cell, sc)
}
