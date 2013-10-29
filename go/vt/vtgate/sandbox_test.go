// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// sandbox_test.go provides a sandbox for unit testing Barnacle.

func init() {
	RegisterDialer("sandbox", sandboxDialer)
}

var (
	// endPointCounter tracks how often GetEndPoints was called
	endPointCounter int

	// endPointMustFail specifies how often GetEndPoints must fail before succeeding
	endPointMustFail int

	// dialerCoun tracks how often sandboxDialer was called
	dialCounter int

	// dialMustFail specifies how often sandboxDialer must fail before succeeding
	dialMustFail int

	// transaction id generator
	transactionId sync2.AtomicInt64
)

func resetSandbox() {
	testConns = make(map[string]TabletConn)
	endPointCounter = 0
	dialCounter = 0
	dialMustFail = 0
}

type sandboxTopo struct {
}

func (sct *sandboxTopo) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	endPointCounter++
	if endPointMustFail > 0 {
		endPointMustFail--
		return nil, fmt.Errorf("topo error")
	}
	return &topo.EndPoints{Entries: []topo.EndPoint{
		{Host: shard, NamedPortMap: map[string]int{"vt": 1}},
	}}, nil
}

var testConns map[string]TabletConn

func sandboxDialer(addr, keyspace, shard, username, password string, encrypted bool) (TabletConn, error) {
	dialCounter++
	if dialMustFail > 0 {
		dialMustFail--
		return nil, OperationalError(fmt.Sprintf("conn error"))
	}
	tconn := testConns[addr]
	if tconn == nil {
		panic(fmt.Sprintf("can't find conn %s", addr))
	}
	return tconn, nil
}

type sandboxConn struct {
	mustFailRetry  int
	mustFailFatal  int
	mustFailServer int
	mustFailConn   int
	mustFailTxPool int
	mustFailNotTx  int
	mustDelay      time.Duration

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount     int
	BeginCount    int
	CommitCount   int
	RollbackCount int
	CloseCount    int

	// TransactionId is auto-generated on Begin
	transactionId int64
}

func (sbc *sandboxConn) getError() error {
	if sbc.mustFailRetry > 0 {
		sbc.mustFailRetry--
		return &ServerError{Code: ERR_RETRY, Err: "retry: err"}
	}
	if sbc.mustFailFatal > 0 {
		sbc.mustFailFatal--
		return &ServerError{Code: ERR_FATAL, Err: "fatal: err"}
	}
	if sbc.mustFailServer > 0 {
		sbc.mustFailServer--
		return &ServerError{Code: ERR_NORMAL, Err: "error: err"}
	}
	if sbc.mustFailConn > 0 {
		sbc.mustFailConn--
		return OperationalError(fmt.Sprintf("error: conn"))
	}
	if sbc.mustFailTxPool > 0 {
		sbc.mustFailTxPool--
		return &ServerError{Code: ERR_TX_POOL_FULL, Err: "tx_pool_full: err"}
	}
	if sbc.mustFailNotTx > 0 {
		sbc.mustFailNotTx--
		return &ServerError{Code: ERR_NOT_IN_TX, Err: "not_in_tx: err"}
	}
	return nil
}

func (sbc *sandboxConn) Execute(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error) {
	sbc.ExecCount++
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return singleRowResult, nil
}

func (sbc *sandboxConn) ExecuteBatch(queries []tproto.BoundQuery) (*tproto.QueryResultList, error) {
	sbc.ExecCount++
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	qrl := &tproto.QueryResultList{List: make([]mproto.QueryResult, 0, len(queries))}
	for _ = range queries {
		qrl.List = append(qrl.List, *singleRowResult)
	}
	return qrl, nil
}

func (sbc *sandboxConn) StreamExecute(query string, bindVars map[string]interface{}) (<-chan *mproto.QueryResult, ErrFunc) {
	sbc.ExecCount++
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	ch := make(chan *mproto.QueryResult, 1)
	ch <- singleRowResult
	close(ch)
	err := sbc.getError()
	return ch, func() error { return err }
}

func (sbc *sandboxConn) Begin() error {
	sbc.ExecCount++
	sbc.BeginCount++
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	err := sbc.getError()
	if err == nil {
		sbc.transactionId = transactionId.Add(1)
	}
	return err
}

func (sbc *sandboxConn) Commit() error {
	sbc.ExecCount++
	sbc.CommitCount++
	sbc.transactionId = 0
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) Rollback() error {
	sbc.ExecCount++
	sbc.RollbackCount++
	sbc.transactionId = 0
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) TransactionId() int64 {
	return sbc.transactionId
}

// Close does not change ExecCount
func (sbc *sandboxConn) Close() error {
	sbc.CloseCount++
	return nil
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
