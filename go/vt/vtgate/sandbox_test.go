// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

// sandbox_test.go provides a sandbox for unit testing VTGate.

func init() {
	tabletconn.RegisterDialer("sandbox", sandboxDialer)
	flag.Set("tablet_protocol", "sandbox")
}

var (
	// Use sandmu to access the variables below
	sandmu sync.Mutex

	// endPointCounter tracks how often GetEndPoints was called
	endPointCounter int

	// endPointMustFail specifies how often GetEndPoints must fail before succeeding
	endPointMustFail int

	// dialerCoun tracks how often sandboxDialer was called
	dialCounter int

	// dialMustFail specifies how often sandboxDialer must fail before succeeding
	dialMustFail int
)

var (
	// transaction id generator
	transactionId sync2.AtomicInt64
)

const (
	TEST_SHARDED               = "TestSharded"
	TEST_UNSHARDED             = "TestUnshared"
	TEST_UNSHARDED_SERVED_FROM = "TestUnshardedServedFrom"
)

func resetSandbox() {
	sandmu.Lock()
	defer sandmu.Unlock()
	testConns = make(map[uint32]tabletconn.TabletConn)
	endPointCounter = 0
	dialCounter = 0
	dialMustFail = 0
	transactionId.Set(0)
}

// sandboxTopo satisfies the SrvTopoServer interface
type sandboxTopo struct {
}

var ShardSpec = "-20-40-60-80-a0-c0-e0-"
var ShardedKrArray key.KeyRangeArray

func getAllShards() (key.KeyRangeArray, error) {
	if ShardedKrArray != nil {
		return ShardedKrArray, nil
	}
	shardedKrArray, err := key.ParseShardingSpec(ShardSpec)
	if err != nil {
		return nil, err
	}
	return shardedKrArray, nil
}

func getKeyRangeName(kr key.KeyRange) string {
	return fmt.Sprintf("%v-%v", string(kr.Start.Hex()), string(kr.End.Hex()))
}

func getUidForShard(shardName string) (int, error) {
	// Try simple unsharded case first
	uid, err := strconv.Atoi(shardName)
	if err == nil {
		return uid, nil
	}
	shards, err := getAllShards()
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

func createShardedSrvKeyspace() (*topo.SrvKeyspace, error) {
	shardKrArray, err := getAllShards()
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

func (sct *sandboxTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	return []string{TEST_SHARDED, TEST_UNSHARDED}, nil
}

func (sct *sandboxTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
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

	return createShardedSrvKeyspace()
}

func (sct *sandboxTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	sandmu.Lock()
	defer sandmu.Unlock()
	endPointCounter++
	if endPointMustFail > 0 {
		endPointMustFail--
		return nil, fmt.Errorf("topo error")
	}
	uid, err := getUidForShard(shard)
	if err != nil {
		panic(err)
	}
	return &topo.EndPoints{Entries: []topo.EndPoint{
		{Uid: uint32(uid), Host: shard, NamedPortMap: map[string]int{"vt": 1}},
	}}, nil
}

var testConns map[uint32]tabletconn.TabletConn

func sandboxDialer(context interface{}, endPoint topo.EndPoint, keyspace, shard string) (tabletconn.TabletConn, error) {
	sandmu.Lock()
	defer sandmu.Unlock()
	dialCounter++
	if dialMustFail > 0 {
		dialMustFail--
		return nil, tabletconn.OperationalError(fmt.Sprintf("conn error"))
	}
	tconn := testConns[endPoint.Uid]
	if tconn == nil {
		panic(fmt.Sprintf("can't find conn %v", endPoint.Uid))
	}
	tconn.(*sandboxConn).endPoint = endPoint
	return tconn, nil
}

func mapTestConn(shard string, conn tabletconn.TabletConn) {
	uid, err := getUidForShard(shard)
	if err != nil {
		panic(err)
	}
	testConns[uint32(uid)] = conn
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

func (sbc *sandboxConn) Execute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return singleRowResult, nil
}

func (sbc *sandboxConn) ExecuteBatch(context interface{}, queries []tproto.BoundQuery, transactionId int64) (*tproto.QueryResultList, error) {
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

func (sbc *sandboxConn) StreamExecute(context interface{}, query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	ch := make(chan *mproto.QueryResult, 1)
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
	return transactionId.Add(1), nil
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
