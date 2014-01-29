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
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

// sandbox_test.go provides a sandbox for unit testing Barnacle.

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

func (sct *sandboxTopo) GetSrvKeyspaceNames(cell string) ([]string, error) {
	panic(fmt.Errorf("not implemented"))
}

func (sct *sandboxTopo) GetSrvKeyspace(cell, keyspace string) (*topo.SrvKeyspace, error) {
	panic(fmt.Errorf("not implemented"))
}

func (sct *sandboxTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	sandmu.Lock()
	defer sandmu.Unlock()
	endPointCounter++
	if endPointMustFail > 0 {
		endPointMustFail--
		return nil, fmt.Errorf("topo error")
	}
	uid, err := strconv.Atoi(shard)
	if err != nil {
		panic(err)
	}
	return &topo.EndPoints{Entries: []topo.EndPoint{
		{Uid: uint32(uid), Host: shard, NamedPortMap: map[string]int{"vt": 1}},
	}}, nil
}

var testConns map[uint32]tabletconn.TabletConn

func sandboxDialer(endPoint topo.EndPoint, keyspace, shard string) (tabletconn.TabletConn, error) {
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

func (sbc *sandboxConn) Execute(query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error) {
	sbc.ExecCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return singleRowResult, nil
}

func (sbc *sandboxConn) ExecuteBatch(queries []tproto.BoundQuery, transactionId int64) (*tproto.QueryResultList, error) {
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

func (sbc *sandboxConn) StreamExecute(query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc) {
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

func (sbc *sandboxConn) Begin() (int64, error) {
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

func (sbc *sandboxConn) Commit(transactionId int64) error {
	sbc.ExecCount.Add(1)
	sbc.CommitCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) Rollback(transactionId int64) error {
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
