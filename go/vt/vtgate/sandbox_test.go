// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/gorpctabletconn"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

// sandbox_test.go provides a sandbox for unit testing VTGate.

const (
	TEST_SHARDED               = "TestSharded"
	TEST_UNSHARDED             = "TestUnshared"
	TEST_UNSHARDED_SERVED_FROM = "TestUnshardedServedFrom"
)

func init() {
	sandboxMap = make(map[string]*sandbox)
	createSandbox(TEST_SHARDED)
	createSandbox(TEST_UNSHARDED)
	tabletconn.RegisterDialer("sandbox", sandboxDialer)
	flag.Set("tablet_protocol", "sandbox")
	tabletconn.AppendResultFuncMap["sandbox"] = gorpctabletconn.AppendResultBson
	tabletconn.MergeResultsFuncMap["sandbox"] = gorpctabletconn.MergeResultsBson
	tabletconn.MergeBatchResultsFuncMap["sandbox"] = gorpctabletconn.MergeBatchResultsBson
}

var sandboxMu sync.Mutex
var sandboxMap map[string]*sandbox

func createSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	s := &sandbox{}
	s.Reset()
	sandboxMap[keyspace] = s
	return s
}

func getSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	return sandboxMap[keyspace]
}

func addSandboxServedFrom(keyspace, servedFrom string) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	sandboxMap[keyspace].KeyspaceServedFrom = servedFrom
	sandboxMap[servedFrom] = sandboxMap[keyspace]
}

type sandbox struct {
	// Use sandmu to access the variables below
	sandmu sync.Mutex

	// SrvKeyspaceCounter tracks how often GetSrvKeyspace was called
	SrvKeyspaceCounter int

	// SrvKeyspaceMustFail specifies how often GetSrvKeyspace must fail before succeeding
	SrvKeyspaceMustFail int

	// EndPointCounter tracks how often GetEndPoints was called
	EndPointCounter int

	// EndPointMustFail specifies how often GetEndPoints must fail before succeeding
	EndPointMustFail int

	// DialerCoun tracks how often sandboxDialer was called
	DialCounter int

	// DialMustFail specifies how often sandboxDialer must fail before succeeding
	DialMustFail int

	// KeyspaceServedFrom specifies the served-from keyspace for vertical resharding
	KeyspaceServedFrom string

	// ShardSpec specifies the sharded keyranges
	ShardSpec string

	// SrvKeyspaceCallback specifies the callback function in GetSrvKeyspace
	SrvKeyspaceCallback func()

	TestConns map[uint32]tabletconn.TabletConn
}

func (s *sandbox) Reset() {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	s.TestConns = make(map[uint32]tabletconn.TabletConn)
	s.SrvKeyspaceCounter = 0
	s.SrvKeyspaceMustFail = 0
	s.EndPointCounter = 0
	s.EndPointMustFail = 0
	s.DialCounter = 0
	s.DialMustFail = 0
	s.KeyspaceServedFrom = ""
	s.ShardSpec = DefaultShardSpec
	s.SrvKeyspaceCallback = nil
}

func (s *sandbox) MapTestConn(shard string, conn tabletconn.TabletConn) {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	uid, err := getUidForShard(shard, s.ShardSpec)
	if err != nil {
		panic(err)
	}
	s.TestConns[uint32(uid)] = conn
}

var DefaultShardSpec = "-20-40-60-80-a0-c0-e0-"

func getAllShards(shardSpec string) (key.KeyRangeArray, error) {
	shardedKrArray, err := key.ParseShardingSpec(shardSpec)
	if err != nil {
		return nil, err
	}
	return shardedKrArray, nil
}

func getKeyRangeName(kr key.KeyRange) string {
	return fmt.Sprintf("%v-%v", string(kr.Start.Hex()), string(kr.End.Hex()))
}

func getUidForShard(shardName string, shardSpec string) (int, error) {
	// Try simple unsharded case first
	if strings.Contains(shardName, "-") == false {
		uid, err := strconv.Atoi(shardName)
		if err == nil {
			return uid, nil
		}
	}
	shards, err := getAllShards(shardSpec)
	if err != nil {
		return 0, fmt.Errorf("shard not found %v", shardName)
	}
	for i, s := range shards {
		if shardName == getKeyRangeName(s) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("shard not found %v", shardName)
}

func createShardedSrvKeyspace(shardSpec, servedFromKeyspace string) (*topo.SrvKeyspace, error) {
	shardKrArray, err := getAllShards(shardSpec)
	if err != nil {
		return nil, err
	}
	allTabletTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	shards := make([]topo.SrvShard, 0, len(shardKrArray))
	for i := 0; i < len(shardKrArray); i++ {
		shard := topo.SrvShard{
			KeyRange:    shardKrArray[i],
			ServedTypes: allTabletTypes,
			TabletTypes: allTabletTypes,
		}
		shards = append(shards, shard)
	}
	shardedSrvKeyspace := &topo.SrvKeyspace{
		Partitions: map[topo.TabletType]*topo.KeyspacePartition{
			topo.TYPE_MASTER: &topo.KeyspacePartition{
				Shards: shards,
			},
		},
		TabletTypes: allTabletTypes,
	}
	if servedFromKeyspace != "" {
		shardedSrvKeyspace.ServedFrom = map[topo.TabletType]string{
			topo.TYPE_RDONLY: servedFromKeyspace,
			topo.TYPE_MASTER: servedFromKeyspace,
		}
	}
	return shardedSrvKeyspace, nil
}

func createUnshardedKeyspace() (*topo.SrvKeyspace, error) {
	allTabletTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	shard := topo.SrvShard{
		KeyRange:    key.KeyRange{Start: "", End: ""},
		ServedTypes: allTabletTypes,
		TabletTypes: allTabletTypes,
	}

	unshardedSrvKeyspace := &topo.SrvKeyspace{
		Partitions: map[topo.TabletType]*topo.KeyspacePartition{
			topo.TYPE_MASTER: &topo.KeyspacePartition{
				Shards: []topo.SrvShard{shard},
			},
			topo.TYPE_REPLICA: &topo.KeyspacePartition{
				Shards: []topo.SrvShard{shard},
			},
			topo.TYPE_RDONLY: &topo.KeyspacePartition{
				Shards: []topo.SrvShard{shard},
			},
		},
		TabletTypes: []topo.TabletType{topo.TYPE_MASTER},
	}
	return unshardedSrvKeyspace, nil
}

// sandboxTopo satisfies the SrvTopoServer interface
type sandboxTopo struct {
}

func (sct *sandboxTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	keyspaces := make([]string, 0, 1)
	for k := range sandboxMap {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

func (sct *sandboxTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	sand := getSandbox(keyspace)
	if sand.SrvKeyspaceCallback != nil {
		sand.SrvKeyspaceCallback()
	}
	sand.SrvKeyspaceCounter++
	if sand.SrvKeyspaceMustFail > 0 {
		sand.SrvKeyspaceMustFail--
		return nil, fmt.Errorf("topo error GetSrvKeyspace")
	}
	switch keyspace {
	case TEST_UNSHARDED_SERVED_FROM:
		servedFromKeyspace, err := createUnshardedKeyspace()
		if err != nil {
			return nil, err
		}
		servedFromKeyspace.ServedFrom = map[topo.TabletType]string{
			topo.TYPE_RDONLY: TEST_UNSHARDED,
			topo.TYPE_MASTER: TEST_UNSHARDED}
		return servedFromKeyspace, nil
	case TEST_UNSHARDED:
		return createUnshardedKeyspace()
	}

	return createShardedSrvKeyspace(sand.ShardSpec, sand.KeyspaceServedFrom)
}

func (sct *sandboxTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	sand := getSandbox(keyspace)
	sand.EndPointCounter++
	if sand.EndPointMustFail > 0 {
		sand.EndPointMustFail--
		return nil, fmt.Errorf("topo error")
	}
	uid, err := getUidForShard(shard, sand.ShardSpec)
	if err != nil {
		panic(err)
	}
	return &topo.EndPoints{Entries: []topo.EndPoint{
		{Uid: uint32(uid), Host: shard, NamedPortMap: map[string]int{"vt": 1}},
	}}, nil
}

func sandboxDialer(context interface{}, endPoint topo.EndPoint, keyspace, shard string, timeout time.Duration) (tabletconn.TabletConn, error) {
	sand := getSandbox(keyspace)
	sand.sandmu.Lock()
	defer sand.sandmu.Unlock()
	sand.DialCounter++
	if sand.DialMustFail > 0 {
		sand.DialMustFail--
		return nil, tabletconn.OperationalError(fmt.Sprintf("conn error"))
	}
	tconn := sand.TestConns[endPoint.Uid]
	if tconn == nil {
		panic(fmt.Sprintf("can't find conn %v", endPoint.Uid))
	}
	tconn.(*sandboxConn).endPoint = endPoint
	return tconn, nil
}

// sandboxConn satisfies the TabletConn interface
type sandboxConn struct {
	endPoint       topo.EndPoint
	mustFailRetry  int
	mustFailFatal  int
	mustFailServer int
	mustFailConn   int
	mustFailTxPool int
	mustFailNotTx  int
	mustDelay      time.Duration

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount     sync2.AtomicInt64
	BeginCount    sync2.AtomicInt64
	CommitCount   sync2.AtomicInt64
	RollbackCount sync2.AtomicInt64
	CloseCount    sync2.AtomicInt64

	// transaction id generator
	TransactionId sync2.AtomicInt64
}

func (sbc *sandboxConn) getError() error {
	if sbc.mustFailRetry > 0 {
		sbc.mustFailRetry--
		return &tabletconn.ServerError{Code: tabletconn.ERR_RETRY, Err: "retry: err"}
	}
	if sbc.mustFailFatal > 0 {
		sbc.mustFailFatal--
		return &tabletconn.ServerError{Code: tabletconn.ERR_FATAL, Err: "fatal: err"}
	}
	if sbc.mustFailServer > 0 {
		sbc.mustFailServer--
		return &tabletconn.ServerError{Code: tabletconn.ERR_NORMAL, Err: "error: err"}
	}
	if sbc.mustFailConn > 0 {
		sbc.mustFailConn--
		return tabletconn.OperationalError(fmt.Sprintf("error: conn"))
	}
	if sbc.mustFailTxPool > 0 {
		sbc.mustFailTxPool--
		return &tabletconn.ServerError{Code: tabletconn.ERR_TX_POOL_FULL, Err: "tx_pool_full: err"}
	}
	if sbc.mustFailNotTx > 0 {
		sbc.mustFailNotTx--
		return &tabletconn.ServerError{Code: tabletconn.ERR_NOT_IN_TX, Err: "not_in_tx: err"}
	}
	return nil
}

func (sbc *sandboxConn) Execute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (interface{}, error) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return singleRowResult, nil
}

func (sbc *sandboxConn) ExecuteBatch(context interface{}, queries []tproto.BoundQuery, transactionId int64) (interface{}, error) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	qrl := &tproto.QueryResultList{}
	qrl.List = make([]mproto.QueryResult, 0, len(queries))
	for _ = range queries {
		qrl.List = append(qrl.List, *singleRowResult)
	}
	return qrl, nil
}

func (sbc *sandboxConn) StreamExecute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (<-chan interface{}, tabletconn.ErrFunc) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	ch := make(chan interface{}, 1)
	ch <- singleRowResult
	close(ch)
	err := sbc.getError()
	return ch, func() error { return err }
}

func (sbc *sandboxConn) Begin(context interface{}) (int64, error) {
	sbc.ExecCount.Add(1)
	sbc.BeginCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	err := sbc.getError()
	if err != nil {
		return 0, err
	}
	return sbc.TransactionId.Add(1), nil
}

func (sbc *sandboxConn) Commit(context interface{}, transactionId int64) error {
	sbc.ExecCount.Add(1)
	sbc.CommitCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) Rollback(context interface{}, transactionId int64) error {
	sbc.ExecCount.Add(1)
	sbc.RollbackCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

// Close does not change ExecCount
func (sbc *sandboxConn) Close() {
	sbc.CloseCount.Add(1)
}

func (sbc *sandboxConn) EndPoint() topo.EndPoint {
	return sbc.endPoint
}

var singleRowResult = &mproto.QueryResult{
	Fields: []mproto.Field{
		{"id", 3},
		{"value", 253}},
	RowsAffected: 1,
	InsertId:     0,
	Rows: [][]sqltypes.Value{{
		{sqltypes.Numeric("1")},
		{sqltypes.String("foo")},
	}},
}
