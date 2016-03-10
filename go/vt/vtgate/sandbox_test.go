// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// sandbox_test.go provides a sandbox for unit testing VTGate.

const (
	KsTestSharded             = "TestSharded"
	KsTestUnsharded           = "TestUnsharded"
	KsTestUnshardedServedFrom = "TestUnshardedServedFrom"
)

func init() {
	ksToSandbox = make(map[string]*sandbox)
	createSandbox(KsTestSharded)
	createSandbox(KsTestUnsharded)
	tabletconn.RegisterDialer("sandbox", sandboxDialer)
	flag.Set("tablet_protocol", "sandbox")
}

var sandboxMu sync.Mutex
var ksToSandbox map[string]*sandbox

func createSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	s := &sandbox{}
	s.Reset()
	ksToSandbox[keyspace] = s
	return s
}

func getSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	return ksToSandbox[keyspace]
}

func addSandboxServedFrom(keyspace, servedFrom string) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	ksToSandbox[keyspace].KeyspaceServedFrom = servedFrom
	ksToSandbox[servedFrom] = ksToSandbox[keyspace]
}

type sandbox struct {
	// Use sandmu to access the variables below
	sandmu sync.Mutex

	// SrvKeyspaceCounter tracks how often GetSrvKeyspace was called
	SrvKeyspaceCounter int

	// SrvKeyspaceMustFail specifies how often GetSrvKeyspace must fail before succeeding
	SrvKeyspaceMustFail int

	// EndPointCounter tracks how often GetEndPoints was called
	EndPointCounter sync2.AtomicInt64

	// EndPointMustFail specifies how often GetEndPoints must fail before succeeding
	EndPointMustFail int

	// DialCounter tracks how often sandboxDialer was called
	DialCounter int

	// DialMustFail specifies how often sandboxDialer must fail before succeeding
	DialMustFail int

	// DialMustTimeout specifies how often sandboxDialer must time out
	DialMustTimeout int

	// KeyspaceServedFrom specifies the served-from keyspace for vertical resharding
	KeyspaceServedFrom string

	// ShardSpec specifies the sharded keyranges
	ShardSpec string

	// SrvKeyspaceCallback specifies the callback function in GetSrvKeyspace
	SrvKeyspaceCallback func()

	TestConns map[string]map[uint32]tabletconn.TabletConn
}

func (s *sandbox) Reset() {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	s.TestConns = make(map[string]map[uint32]tabletconn.TabletConn)
	s.SrvKeyspaceCounter = 0
	s.SrvKeyspaceMustFail = 0
	s.EndPointCounter.Set(0)
	s.EndPointMustFail = 0
	s.DialCounter = 0
	s.DialMustFail = 0
	s.DialMustTimeout = 0
	s.KeyspaceServedFrom = ""
	s.ShardSpec = DefaultShardSpec
	s.SrvKeyspaceCallback = nil
}

// a sandboxableConn is a tablet.TabletConn that allows you
// to set the endPoint. MapTestConn uses it to set some good
// defaults. This way, you have the option of calling MapTestConn
// with variables other than sandboxConn.
type sandboxableConn interface {
	tabletconn.TabletConn
	setEndPoint(*topodatapb.EndPoint)
}

func (s *sandbox) MapTestConn(shard string, conn sandboxableConn) {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	conns, ok := s.TestConns[shard]
	if !ok {
		conns = make(map[uint32]tabletconn.TabletConn)
	}
	uid := uint32(len(conns))
	conn.setEndPoint(&topodatapb.EndPoint{
		Uid:     uid,
		Host:    shard,
		PortMap: map[string]int32{"vt": 1},
	})
	conns[uid] = conn
	s.TestConns[shard] = conns
}

func (s *sandbox) DeleteTestConn(shard string, conn tabletconn.TabletConn) {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	conns, ok := s.TestConns[shard]
	if !ok {
		panic(fmt.Sprintf("unknown shard: %v", shard))
	}
	delete(conns, conn.EndPoint().Uid)
	s.TestConns[shard] = conns
}

var DefaultShardSpec = "-20-40-60-80-a0-c0-e0-"

func getAllShards(shardSpec string) ([]*topodatapb.KeyRange, error) {
	shardedKrArray, err := key.ParseShardingSpec(shardSpec)
	if err != nil {
		return nil, err
	}
	return shardedKrArray, nil
}

func createShardedSrvKeyspace(shardSpec, servedFromKeyspace string) (*topodatapb.SrvKeyspace, error) {
	shardKrArray, err := getAllShards(shardSpec)
	if err != nil {
		return nil, err
	}
	shards := make([]*topodatapb.ShardReference, 0, len(shardKrArray))
	for i := 0; i < len(shardKrArray); i++ {
		shard := &topodatapb.ShardReference{
			Name:     key.KeyRangeString(shardKrArray[i]),
			KeyRange: shardKrArray[i],
		}
		shards = append(shards, shard)
	}
	shardedSrvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id", // exact value is ignored
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType:      topodatapb.TabletType_MASTER,
				ShardReferences: shards,
			},
			{
				ServedType:      topodatapb.TabletType_REPLICA,
				ShardReferences: shards,
			},
			{
				ServedType:      topodatapb.TabletType_RDONLY,
				ShardReferences: shards,
			},
		},
	}
	if servedFromKeyspace != "" {
		shardedSrvKeyspace.ServedFrom = []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   servedFromKeyspace,
			},
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   servedFromKeyspace,
			},
		}
	}
	return shardedSrvKeyspace, nil
}

func createUnshardedKeyspace() (*topodatapb.SrvKeyspace, error) {
	shard := &topodatapb.ShardReference{
		Name: "0",
	}

	unshardedSrvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType:      topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
			{
				ServedType:      topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
			{
				ServedType:      topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
		},
	}
	return unshardedSrvKeyspace, nil
}

// sandboxTopo satisfies the SrvTopoServer interface
type sandboxTopo struct {
	callbackGetEndPoints func(st *sandboxTopo)
}

func (sct *sandboxTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	keyspaces := make([]string, 0, 1)
	for k := range ksToSandbox {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

func (sct *sandboxTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
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
	case KsTestUnshardedServedFrom:
		servedFromKeyspace, err := createUnshardedKeyspace()
		if err != nil {
			return nil, err
		}
		servedFromKeyspace.ServedFrom = []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   KsTestUnsharded,
			},
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   KsTestUnsharded,
			},
		}
		return servedFromKeyspace, nil
	case KsTestUnsharded:
		return createUnshardedKeyspace()
	}

	return createShardedSrvKeyspace(sand.ShardSpec, sand.KeyspaceServedFrom)
}

func (sct *sandboxTopo) GetSrvShard(ctx context.Context, cell, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return nil, fmt.Errorf("Unsupported")
}

func (sct *sandboxTopo) GetEndPoints(ctx context.Context, cell, keyspace, shard string, tabletType topodatapb.TabletType) (*topodatapb.EndPoints, int64, error) {
	sand := getSandbox(keyspace)
	sand.EndPointCounter.Add(1)
	if sct.callbackGetEndPoints != nil {
		sct.callbackGetEndPoints(sct)
	}
	if sand.EndPointMustFail > 0 {
		sand.EndPointMustFail--
		return nil, -1, fmt.Errorf("topo error")
	}

	conns := sand.TestConns[shard]
	ep := &topodatapb.EndPoints{}
	for _, conn := range conns {
		ep.Entries = append(ep.Entries, conn.EndPoint())
	}
	return ep, -1, nil
}

func sandboxDialer(ctx context.Context, endPoint *topodatapb.EndPoint, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	sand := getSandbox(keyspace)
	sand.sandmu.Lock()
	defer sand.sandmu.Unlock()
	sand.DialCounter++
	if sand.DialMustFail > 0 {
		sand.DialMustFail--
		return nil, tabletconn.OperationalError(fmt.Sprintf("conn error"))
	}
	if sand.DialMustTimeout > 0 {
		time.Sleep(timeout)
		sand.DialMustTimeout--
		return nil, tabletconn.OperationalError(fmt.Sprintf("conn unreachable"))
	}
	conns := sand.TestConns[shard]
	if conns == nil {
		panic(fmt.Sprintf("can't find shard %v", shard))
	}
	tconn := conns[endPoint.Uid]
	return tconn, nil
}

// sandboxConn satisfies the TabletConn interface
type sandboxConn struct {
	endPoint       *topodatapb.EndPoint
	mustFailRetry  int
	mustFailFatal  int
	mustFailServer int
	mustFailConn   int
	mustFailTxPool int
	mustFailNotTx  int
	mustDelay      time.Duration

	// A callback to tweak the behavior on each conn call
	onConnUse func(*sandboxConn)

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount          sync2.AtomicInt64
	BeginCount         sync2.AtomicInt64
	CommitCount        sync2.AtomicInt64
	RollbackCount      sync2.AtomicInt64
	CloseCount         sync2.AtomicInt64
	AsTransactionCount sync2.AtomicInt64

	// Queries stores the non-batch requests received.
	Queries []querytypes.BoundQuery

	// BatchQueries stores the batch requests received
	// Each batch request is inlined as a slice of Queries.
	BatchQueries [][]querytypes.BoundQuery

	// results specifies the results to be returned.
	// They're consumed as results are returned. If there are
	// no results left, singleRowResult is returned.
	results []*sqltypes.Result

	// transaction id generator
	TransactionID sync2.AtomicInt64
}

func (sbc *sandboxConn) getError() error {
	if sbc.onConnUse != nil {
		sbc.onConnUse(sbc)
	}
	if sbc.mustFailRetry > 0 {
		sbc.mustFailRetry--
		return &tabletconn.ServerError{
			Code:       tabletconn.ERR_RETRY,
			Err:        "retry: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	if sbc.mustFailFatal > 0 {
		sbc.mustFailFatal--
		return &tabletconn.ServerError{
			Code:       tabletconn.ERR_FATAL,
			Err:        "fatal: err",
			ServerCode: vtrpcpb.ErrorCode_INTERNAL_ERROR,
		}
	}
	if sbc.mustFailServer > 0 {
		sbc.mustFailServer--
		return &tabletconn.ServerError{
			Code:       tabletconn.ERR_NORMAL,
			Err:        "error: err",
			ServerCode: vtrpcpb.ErrorCode_BAD_INPUT,
		}
	}
	if sbc.mustFailConn > 0 {
		sbc.mustFailConn--
		return tabletconn.OperationalError(fmt.Sprintf("error: conn"))
	}
	if sbc.mustFailTxPool > 0 {
		sbc.mustFailTxPool--
		return &tabletconn.ServerError{
			Code:       tabletconn.ERR_TX_POOL_FULL,
			Err:        "tx_pool_full: err",
			ServerCode: vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED,
		}
	}
	if sbc.mustFailNotTx > 0 {
		sbc.mustFailNotTx--
		return &tabletconn.ServerError{
			Code:       tabletconn.ERR_NOT_IN_TX,
			Err:        "not_in_tx: err",
			ServerCode: vtrpcpb.ErrorCode_NOT_IN_TX,
		}
	}
	return nil
}

func (sbc *sandboxConn) setResults(r []*sqltypes.Result) {
	sbc.results = r
}

func (sbc *sandboxConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]interface{})
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, querytypes.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return sbc.getNextResult(), nil
}

func (sbc *sandboxConn) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
	return sbc.Execute(ctx, query, bindVars, transactionID)
}

func (sbc *sandboxConn) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	if asTransaction {
		sbc.AsTransactionCount.Add(1)
	}
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	sbc.BatchQueries = append(sbc.BatchQueries, queries)
	result := make([]sqltypes.Result, 0, len(queries))
	for range queries {
		result = append(result, *(sbc.getNextResult()))
	}
	return result, nil
}

func (sbc *sandboxConn) ExecuteBatch2(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	return sbc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

func (sbc *sandboxConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]interface{})
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, querytypes.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	ch := make(chan *sqltypes.Result, 1)
	ch <- sbc.getNextResult()
	close(ch)
	err := sbc.getError()
	return ch, func() error { return err }, err
}

func (sbc *sandboxConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	return sbc.StreamExecute(ctx, query, bindVars, transactionID)
}

func (sbc *sandboxConn) Begin(ctx context.Context) (int64, error) {
	sbc.ExecCount.Add(1)
	sbc.BeginCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	err := sbc.getError()
	if err != nil {
		return 0, err
	}
	return sbc.TransactionID.Add(1), nil
}

func (sbc *sandboxConn) Begin2(ctx context.Context) (int64, error) {
	return sbc.Begin(ctx)
}

func (sbc *sandboxConn) Commit(ctx context.Context, transactionID int64) error {
	sbc.ExecCount.Add(1)
	sbc.CommitCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) Commit2(ctx context.Context, transactionID int64) error {
	return sbc.Commit(ctx, transactionID)
}

func (sbc *sandboxConn) Rollback(ctx context.Context, transactionID int64) error {
	sbc.ExecCount.Add(1)
	sbc.RollbackCount.Add(1)
	if sbc.mustDelay != 0 {
		time.Sleep(sbc.mustDelay)
	}
	return sbc.getError()
}

func (sbc *sandboxConn) Rollback2(ctx context.Context, transactionID int64) error {
	return sbc.Rollback(ctx, transactionID)
}

var sandboxSQRowCount = int64(10)

// Fake SplitQuery creates splits from the original query by appending the
// split index as a comment to the SQL. RowCount is always sandboxSQRowCount
func (sbc *sandboxConn) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) ([]querytypes.QuerySplit, error) {
	splits := []querytypes.QuerySplit{}
	for i := 0; i < int(splitCount); i++ {
		split := querytypes.QuerySplit{
			Sql:           fmt.Sprintf("%s /*split %v */", query.Sql, i),
			BindVariables: query.BindVariables,
			RowCount:      sandboxSQRowCount,
		}
		splits = append(splits, split)
	}
	return splits, nil
}

// StreamHealth is not implemented.
func (sbc *sandboxConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return nil, fmt.Errorf("Not implemented in test")
}

// Close does not change ExecCount
func (sbc *sandboxConn) Close() {
	sbc.CloseCount.Add(1)
}

func (sbc *sandboxConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	return fmt.Errorf("not implemented, vtgate doesn't use target yet")
}

func (sbc *sandboxConn) EndPoint() *topodatapb.EndPoint {
	return sbc.endPoint
}

func (sbc *sandboxConn) setEndPoint(ep *topodatapb.EndPoint) {
	sbc.endPoint = ep
}

func (sbc *sandboxConn) getNextResult() *sqltypes.Result {
	if len(sbc.results) != 0 {
		r := sbc.results[0]
		sbc.results = sbc.results[1:]
		return r
	}
	return singleRowResult
}

var singleRowResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{"id", sqltypes.Int32},
		{"value", sqltypes.VarChar},
	},
	RowsAffected: 1,
	InsertID:     0,
	Rows: [][]sqltypes.Value{{
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo")),
	}},
}
