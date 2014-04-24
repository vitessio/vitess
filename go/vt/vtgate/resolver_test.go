// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package vtgate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

// This file uses the sandbox_test framework.

func TestResolverExecuteKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyspaceIdQuery{
			Sql:         "query",
			Keyspace:    "TestResolverExecuteKeyspaceIds",
			KeyspaceIds: []key.KeyspaceId{kid10, kid25},
			TabletType:  topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		return res.ExecuteKeyspaceIds(nil, query)
	})
}

func TestResolverExecuteKeyRanges(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyRangeQuery{
			Sql:        "query",
			Keyspace:   "TestResolverExecuteKeyRanges",
			KeyRanges:  []key.KeyRange{key.KeyRange{Start: kid10, End: kid25}},
			TabletType: topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		return res.ExecuteKeyRanges(nil, query)
	})
}

func TestResolverExecuteEntityIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteEntityIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.EntityIdsQuery{
			Sql:              "query",
			Keyspace:         "TestResolverExecuteEntityIds",
			EntityColumnName: "col",
			EntityKeyspaceIdMap: map[string]key.KeyspaceId{
				"0": kid10,
				"1": kid25,
			},
			TabletType: topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		return res.ExecuteEntityIds(nil, query)
	})
}

func TestResolverExecuteBatchKeyspaceIds(t *testing.T) {
	testResolverGeneric(t, "TestResolverExecuteBatchKeyspaceIds", func() (*mproto.QueryResult, error) {
		kid10, err := key.HexKeyspaceId("10").Unhex()
		if err != nil {
			return nil, err
		}
		kid25, err := key.HexKeyspaceId("25").Unhex()
		if err != nil {
			return nil, err
		}
		query := &proto.KeyspaceIdBatchQuery{
			Queries:     []tproto.BoundQuery{{"query", nil}},
			Keyspace:    "TestResolverExecuteBatchKeyspaceIds",
			KeyspaceIds: []key.KeyspaceId{kid10, kid25},
			TabletType:  topo.TYPE_MASTER,
		}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		qrs, err := res.ExecuteBatchKeyspaceIds(nil, query)
		if err != nil {
			return nil, err
		}
		return &qrs.List[0], err
	})
}

func TestResolverStreamExecuteKeyspaceIds(t *testing.T) {
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid25, err := key.HexKeyspaceId("25").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	query := &proto.KeyspaceIdQuery{
		Sql:         "query",
		Keyspace:    "TestResolverStreamExecuteKeyspaceIds",
		KeyspaceIds: []key.KeyspaceId{kid10, kid15},
		TabletType:  topo.TYPE_MASTER,
	}
	createSandbox("TestResolverStreamExecuteKeyspaceIds")
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyspaceIds(nil, query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyspaceIds", func() (*mproto.QueryResult, error) {
		query.KeyspaceIds = []key.KeyspaceId{kid10, kid15, kid25}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyspaceIds(nil, query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func TestResolverStreamExecuteKeyRanges(t *testing.T) {
	kid10, err := key.HexKeyspaceId("10").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid15, err := key.HexKeyspaceId("15").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	kid25, err := key.HexKeyspaceId("25").Unhex()
	if err != nil {
		t.Errorf(err.Error())
	}
	query := &proto.KeyRangeQuery{
		Sql:        "query",
		Keyspace:   "TestResolverStreamExecuteKeyRanges",
		KeyRanges:  []key.KeyRange{key.KeyRange{Start: kid10, End: kid15}},
		TabletType: topo.TYPE_MASTER,
	}
	createSandbox("TestResolverStreamExecuteKeyRanges")
	// streaming a single shard
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyRanges(nil, query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
	// streaming multiple shards
	testResolverStreamGeneric(t, "TestResolverStreamExecuteKeyRanges", func() (*mproto.QueryResult, error) {
		query.KeyRanges = []key.KeyRange{key.KeyRange{Start: kid10, End: kid25}}
		res := NewResolver(new(sandboxTopo), "aa", 1*time.Millisecond, 0, 1*time.Millisecond)
		qr := new(mproto.QueryResult)
		err = res.StreamExecuteKeyRanges(nil, query, func(r *mproto.QueryResult) error {
			appendResult(qr, r)
			return nil
		})
		return qr, err
	})
}

func testResolverGeneric(t *testing.T, name string, action func() (*mproto.QueryResult, error)) {
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
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}

	// non-retryable failure
	s.Reset()
	sbc0 = &sandboxConn{mustFailServer: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want := fmt.Sprintf("error: err, shard, host: %s.-20.master, {Uid:0 Host:-20 NamedPortMap:map[vt:1] Health:map[]}\nretry: err, shard, host: %s.20-40.master, {Uid:1 Host:20-40 NamedPortMap:map[vt:1] Health:map[]}", name, name)
	if err == nil || err.Error() != want {
		t.Errorf("want\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
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
	want = fmt.Sprintf("retry: err, shard, host: %s.-20.master, {Uid:0 Host:-20 NamedPortMap:map[vt:1] Health:map[]}\nfatal: err, shard, host: %s.20-40.master, {Uid:1 Host:20-40 NamedPortMap:map[vt:1] Health:map[]}", name, name)
	if err == nil || err.Error() != want {
		t.Errorf("want\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once.
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc1.ExecCount)
	}
	// Ensure that we tried topo only 3 times
	if s.SrvKeyspaceCounter != 3 {
		t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
	}

	// retryable failure, vertical resharding
	s.Reset()
	addSandboxServedFrom(name, name+"ServedFrom")
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailFatal: 1}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	if err != nil {
		t.Errorf("want nil, got %v", err)
	}
	// Ensure that we tried only twice.
	if sbc0.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc1.ExecCount)
	}
	// Ensure that we tried topo only 3 times
	if s.SrvKeyspaceCounter != 3 {
		t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
	}

	// retryable failure, horizontal resharding
	s.Reset()
	sbc0 = &sandboxConn{}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("20-40", sbc1)
	i := 0
	s.SrvKeyspaceCallback = func() {
		if i > 0 {
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
	if sbc0.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc0.ExecCount)
	}
	if sbc1.ExecCount != 2 {
		t.Errorf("want 2, got %v", sbc1.ExecCount)
	}
	// Ensure that we tried topo only 3 times
	if s.SrvKeyspaceCounter != 3 {
		t.Errorf("want 3, got %v", s.SrvKeyspaceCounter)
	}
}

func testResolverStreamGeneric(t *testing.T, name string, action func() (*mproto.QueryResult, error)) {
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
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
	}

	// failure
	s.Reset()
	sbc0 = &sandboxConn{mustFailRetry: 1}
	s.MapTestConn("-20", sbc0)
	sbc1 = &sandboxConn{}
	s.MapTestConn("20-40", sbc1)
	_, err = action()
	want := fmt.Sprintf("retry: err, shard, host: %s.-20.master, {Uid:0 Host:-20 NamedPortMap:map[vt:1] Health:map[]}", name)
	if err == nil || err.Error() != want {
		t.Errorf("want\n%s\ngot\n%v", want, err)
	}
	// Ensure that we tried only once.
	if sbc0.ExecCount != 1 {
		t.Errorf("want 1, got %v", sbc0.ExecCount)
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
		got := insertSqlClause(test[0], clause)
		if got != test[1] {
			t.Errorf("want '%v', got '%v'", test[1], got)
		}
	}
}

func TestResolverBuildEntityIds(t *testing.T) {
	shardMap := make(map[string][]string)
	shardMap["-20"] = []string{"0", "1"}
	shardMap["20-40"] = []string{"2"}
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
		"-20":   map[string]interface{}{"id": 10, "uid0": "0", "uid1": "1"},
		"20-40": map[string]interface{}{"id": 10, "uid0": "2"},
	}
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
