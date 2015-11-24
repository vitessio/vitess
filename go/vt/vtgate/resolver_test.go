// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// This file uses the sandbox_test framework.

func TestResolverExecuteKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyspaceIds", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		return res.ExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyspaceIds",
			[][]byte{[]byte{0x10}, []byte{0x25}},
			topodatapb.TabletType_MASTER,
			nil,
			false)
	})
}

func TestResolverExecuteKeyRanges(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyRanges", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		return res.ExecuteKeyRanges(context.Background(),
			"query",
			nil,
			"TestResolverExecuteKeyRanges",
			[]*topodatapb.KeyRange{&topodatapb.KeyRange{Start: []byte{0x10}, End: []byte{0x25}}},
			topodatapb.TabletType_MASTER,
			nil,
			false)
	})
}

func TestResolverExecuteEntityIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteEntityIds", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		return res.ExecuteEntityIds(context.Background(),
			"query",
			nil,
			"TestResolverExecuteEntityIds",
			"col",
			[]*vtgatepb.ExecuteEntityIdsRequest_EntityId{
				&vtgatepb.ExecuteEntityIdsRequest_EntityId{
					XidType:    sqltypes.Int64,
					XidValue:   []byte("0"),
					KeyspaceId: []byte{0x10},
				},
				&vtgatepb.ExecuteEntityIdsRequest_EntityId{
					XidType:    sqltypes.VarBinary,
					XidValue:   []byte("1"),
					KeyspaceId: []byte{0x25},
				},
			},
			topodatapb.TabletType_MASTER,
			nil,
			false)
	})
}

func TestResolverExecuteBatchKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteBatchKeyspaceIds", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		qrs, err := res.ExecuteBatchKeyspaceIds(context.Background(),
			[]*vtgatepb.BoundKeyspaceIdQuery{{
				Query: &querypb.BoundQuery{
					Sql:           "query",
					BindVariables: nil,
				},
				Keyspace: "TestResolverExecuteBatchKeyspaceIds",
				KeyspaceIds: [][]byte{
					[]byte{0x10},
					[]byte{0x25},
				},
			}},
			topodatapb.TabletType_MASTER,
			false,
			nil)
		if err != nil {
			return nil, err
		}
		return &qrs[0], err
	})
}

func TestResolverStreamExecuteKeyspaceIds(t *testing.T) {
	createSandbox("TestResolverStreamExecuteKeyspaceIds")
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		qr := new(sqltypes.Result)
		err := res.StreamExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			"TestResolverStreamExecuteKeyspaceIds",
			[][]byte{[]byte{0x10}, []byte{0x15}},
			topodatapb.TabletType_MASTER,
			func(r *sqltypes.Result) error {
				appendResult(qr, r)
				return nil
			})
		return qr, err
	})
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		qr := new(sqltypes.Result)
		err := res.StreamExecuteKeyspaceIds(context.Background(),
			"query",
			nil,
			"TestResolverStreamExecuteKeyspaceIds",
			[][]byte{[]byte{0x10}, []byte{0x15}, []byte{0x25}},
			topodatapb.TabletType_MASTER,
			func(r *sqltypes.Result) error {
				appendResult(qr, r)
				return nil
			})
		return qr, err
	})
}

func TestResolverStreamExecuteKeyRanges(t *testing.T) {
	createSandbox("TestResolverStreamExecuteKeyRanges")
	// streaming a single shard
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		qr := new(sqltypes.Result)
		err := res.StreamExecuteKeyRanges(context.Background(),
			"query",
			nil,
			"TestResolverStreamExecuteKeyRanges",
			[]*topodatapb.KeyRange{&topodatapb.KeyRange{Start: []byte{0x10}, End: []byte{0x15}}},
			topodatapb.TabletType_MASTER,
			func(r *sqltypes.Result) error {
				appendResult(qr, r)
				return nil
			})
		return qr, err
	})
	// streaming multiple shards
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*sqltypes.Result, error) {
		res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")
		qr := new(sqltypes.Result)
		err := res.StreamExecuteKeyRanges(context.Background(),
			"query",
			nil,
			"TestResolverStreamExecuteKeyRanges",
			[]*topodatapb.KeyRange{&topodatapb.KeyRange{Start: []byte{0x10}, End: []byte{0x25}}},
			topodatapb.TabletType_MASTER,
			func(r *sqltypes.Result) error {
				appendResult(qr, r)
				return nil
			})
		return qr, err
	})
}

func testResolverGeneric(t *testing.T, name string, action func() (*sqltypes.Result, error)) {
	// successful execute
	s := createSandbox(name)
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err := action()
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
	sbc0 = &sandboxConn{mustFailServer: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want1 := fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , error: err", name)
	want2 := fmt.Sprintf("shard, host: %s.20-40.master, host:\"20-40\" port_map:<key:\"vt\" value:1 > , retry: err", name)
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
	sbc0 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want1 = fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , retry: err", name)
	want2 = fmt.Sprintf("shard, host: %s.20-40.master, host:\"20-40\" port_map:<key:\"vt\" value:1 > , fatal: err", name)
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
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	s0 := createSandbox(name + "ServedFrom0") // make sure we have a fresh copy
	s0.ShardSpec = "-80-"
	sbc2 := &sandboxConn{}
	s0.MapTestConn("-80", sbc2)
	_, err = action()
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
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("20-40", sbc1)
	i := 0
	s.SrvKeyspaceCallback = func() {
		if i == 1 {
			addSandboxServedFrom(name, name+"ServedFrom")
		}
		i++
	}
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried only twice.
	if execCount := sbc0.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	// Ensure that we tried topo only 3 times.
	if s.SrvKeyspaceCounter != 3 {
		t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
	}

	// retryable failure, horizontal resharding
	s.Reset()
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	i = 0
	s.SrvKeyspaceCallback = func() {
		if i == 1 {
			s.ShardSpec = "-20-30-40-60-80-a0-c0-e0-"
			s.MapTestConn("-20", sbc0)
			s.MapTestConn("20-30", sbc1)
		}
		i++
	}
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried only twice.
	if execCount := sbc0.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	if execCount := sbc1.ExecCount.Get(); execCount != 2 {
		t.Errorf("want 2, got %v", execCount)
	}
	// Ensure that we tried topo only twice.
	if s.SrvKeyspaceCounter != 2 {
		t.Errorf("want 2, got %v", s.SrvKeyspaceCounter)
	}
}

func testResolverStreamGeneric(t *testing.T, name string, action func() (*sqltypes.Result, error)) {
	// successful execute
	s := createSandbox(name)
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err := action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	if execCount := sbc0.ExecCount.Get(); execCount != 1 {
		t.Errorf("want 1, got %v", execCount)
	}

	// failure
	s.Reset()
	sbc0 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want := fmt.Sprintf("shard, host: %s.-20.master, host:\"-20\" port_map:<key:\"vt\" value:1 > , retry: err", name)
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

func TestResolverInsertSqlClause(t *testing.T) {
	clause := "col in (:col1, :col2)"
	tests := [][]string{
		[]string{
			"select a from table",
			"select a from table where " + clause},
		[]string{
			"select a from table where id = 1",
			"select a from table where id = 1 and " + clause},
		[]string{
			"select a from table group by a",
			"select a from table where " + clause + " group by a"},
		[]string{
			"select a from table where id = 1 order by a limit 10",
			"select a from table where id = 1 and " + clause + " order by a limit 10"},
		[]string{
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
	shardMap := make(map[string][]interface{})
	shardMap["-20"] = []interface{}{"0", 1}
	shardMap["20-40"] = []interface{}{"2"}
	sql := "select a from table where id=:id"
	entityColName := "uid"
	bindVar := make(map[string]interface{})
	bindVar["id"] = 10
	shards, sqls, bindVars := buildEntityIds(shardMap, sql, entityColName, bindVar)
	wantShards := []string{"-20", "20-40"}
	wantSqls := map[string]string{
		"-20":   "select a from table where id=:id and uid in (:uid0, :uid1)",
		"20-40": "select a from table where id=:id and uid in (:uid0)",
	}
	wantBindVars := map[string]map[string]interface{}{
		"-20":   map[string]interface{}{"id": 10, "uid0": "0", "uid1": 1},
		"20-40": map[string]interface{}{"id": 10, "uid0": "2"},
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
		t.Errorf("want %+v, got %+v", wantBindVars, bindVars)
	}
}

func TestResolverDmlOnMultipleKeyspaceIds(t *testing.T) {
	res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")

	s := createSandbox("TestResolverDmlOnMultipleKeyspaceIds")
	sbc0 := &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 := &sandboxConn{}
	s.MapTestConn("20-40", sbc1)

	errStr := "DML should not span multiple keyspace_ids"
	_, err := res.ExecuteKeyspaceIds(context.Background(),
		"update table set a = b",
		nil,
		"TestResolverExecuteKeyspaceIds",
		[][]byte{[]byte{0x10}, []byte{0x25}},
		topodatapb.TabletType_MASTER,
		nil,
		false)
	if err == nil {
		t.Errorf("want %v, got nil", errStr)
	}
}

func TestResolverExecBatchReresolve(t *testing.T) {
	s := createSandbox("TestResolverExecBatchReresolve")
	sbc := &sandboxConn{mustFailRetry: 20}
	s.MapTestConn("0", sbc)

	res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")

	callcount := 0
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		callcount++
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: "TestResolverExecBatchReresolve",
			Shards:   []string{"0"},
		}}
		return boundShardQueriesToScatterBatchRequest(queries)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, false, nil, buildBatchRequest)
	want := "shard, host: TestResolverExecBatchReresolve.0.master, host:\"0\" port_map:<key:\"vt\" value:1 > , retry: err"
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
	s := createSandbox("TestResolverExecBatchAsTransaction")
	sbc := &sandboxConn{mustFailRetry: 20}
	s.MapTestConn("0", sbc)

	res := NewResolver(nil, topo.Server{}, new(sandboxTopo), "", "aa", retryDelay, 0, connTimeoutTotal, connTimeoutPerConn, connLife, "")

	callcount := 0
	buildBatchRequest := func() (*scatterBatchRequest, error) {
		callcount++
		queries := []*vtgatepb.BoundShardQuery{{
			Query: &querypb.BoundQuery{
				Sql:           "query",
				BindVariables: nil,
			},
			Keyspace: "TestResolverExecBatchAsTransaction",
			Shards:   []string{"0"},
		}}
		return boundShardQueriesToScatterBatchRequest(queries)
	}

	_, err := res.ExecuteBatch(context.Background(), topodatapb.TabletType_MASTER, true, nil, buildBatchRequest)
	want := "shard, host: TestResolverExecBatchAsTransaction.0.master, host:\"0\" port_map:<key:\"vt\" value:1 > , retry: err"
	if err == nil || err.Error() != want {
		t.Errorf("want got, got none")
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

func TestIsConnError(t *testing.T) {
	var connErrorTests = []struct {
		in      error
		outCode int
		outBool bool
	}{
		{fmt.Errorf("generic error"), 0, false},
		{&ScatterConnError{Code: 9}, 9, true},
		{&ShardConnError{Code: 9}, 9, true},
		{&tabletconn.ServerError{Code: 9}, 0, false},
	}

	for _, tt := range connErrorTests {
		gotCode, gotBool := isConnError(tt.in)
		if (gotCode != tt.outCode) || (gotBool != tt.outBool) {
			t.Errorf("isConnError(%v) => (%v, %v), want (%v, %v)",
				tt.in, gotCode, gotBool, tt.outCode, tt.outBool)
		}
	}
}
