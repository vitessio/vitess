// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
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
	s := &sandbox{VSchema: "{}"}
	s.Reset()
	ksToSandbox[keyspace] = s
	return s
}

func getSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	return ksToSandbox[keyspace]
}

func getSandboxSrvVSchema() *vschemapb.SrvVSchema {
	result := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{},
	}
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	for keyspace, sandbox := range ksToSandbox {
		var vs vschemapb.Keyspace
		_ = json.Unmarshal([]byte(sandbox.VSchema), &vs)
		result.Keyspaces[keyspace] = &vs
	}
	return result
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

	// VSchema specifies the vschema in JSON format.
	VSchema string
}

// Reset cleans up sandbox internal state.
func (s *sandbox) Reset() {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	s.SrvKeyspaceCounter = 0
	s.SrvKeyspaceMustFail = 0
	s.DialCounter = 0
	s.DialMustFail = 0
	s.DialMustTimeout = 0
	s.KeyspaceServedFrom = ""
	s.ShardSpec = DefaultShardSpec
	s.SrvKeyspaceCallback = nil
}

// DefaultShardSpec is the default sharding scheme for testing.
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
}

// GetSrvKeyspaceNames is part of SrvTopoServer.
func (sct *sandboxTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	keyspaces := make([]string, 0, 1)
	for k := range ksToSandbox {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

// GetSrvKeyspace is part of SrvTopoServer.
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

// WatchSrvVSchema is part of SrvTopoServer.
func (sct *sandboxTopo) WatchSrvVSchema(ctx context.Context, cell string) (notifications <-chan *vschemapb.SrvVSchema, err error) {
	result := make(chan *vschemapb.SrvVSchema, 1)
	value := getSandboxSrvVSchema()
	result <- value
	return result, nil
}

func sandboxDialer(ctx context.Context, tablet *topodatapb.Tablet, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
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
	sbc := &sandboxConn{}
	sbc.tablet = tablet
	sbc.SetTarget(keyspace, shard, tabletType)
	return sbc, nil
}

// sandboxConn satisfies the TabletConn interface
type sandboxConn struct {
	keyspace   string
	shard      string
	tabletType topodatapb.TabletType
	tablet     *topodatapb.Tablet

	mustFailRetry  int
	mustFailFatal  int
	mustFailServer int
	mustFailConn   int
	mustFailTxPool int
	mustFailNotTx  int

	// These Count vars report how often the corresponding
	// functions were called.
	ExecCount          sync2.AtomicInt64
	BeginCount         sync2.AtomicInt64
	CommitCount        sync2.AtomicInt64
	RollbackCount      sync2.AtomicInt64
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
	if sbc.mustFailRetry > 0 {
		sbc.mustFailRetry--
		return &tabletconn.ServerError{
			Err:        "retry: err",
			ServerCode: vtrpcpb.ErrorCode_QUERY_NOT_SERVED,
		}
	}
	if sbc.mustFailFatal > 0 {
		sbc.mustFailFatal--
		return &tabletconn.ServerError{
			Err:        "fatal: err",
			ServerCode: vtrpcpb.ErrorCode_INTERNAL_ERROR,
		}
	}
	if sbc.mustFailServer > 0 {
		sbc.mustFailServer--
		return &tabletconn.ServerError{
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
			Err:        "tx_pool_full: err",
			ServerCode: vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED,
		}
	}
	if sbc.mustFailNotTx > 0 {
		sbc.mustFailNotTx--
		return &tabletconn.ServerError{
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
	if err := sbc.getError(); err != nil {
		return nil, err
	}
	return sbc.getNextResult(), nil
}

func (sbc *sandboxConn) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	sbc.ExecCount.Add(1)
	if asTransaction {
		sbc.AsTransactionCount.Add(1)
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

type streamExecuteAdapter struct {
	result *sqltypes.Result
	done   bool
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	if a.done {
		return nil, io.EOF
	}
	a.done = true
	return a.result, nil
}

func (sbc *sandboxConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error) {
	sbc.ExecCount.Add(1)
	bv := make(map[string]interface{})
	for k, v := range bindVars {
		bv[k] = v
	}
	sbc.Queries = append(sbc.Queries, querytypes.BoundQuery{
		Sql:           query,
		BindVariables: bv,
	})
	err := sbc.getError()
	if err != nil {
		return nil, err
	}
	r := sbc.getNextResult()
	return &streamExecuteAdapter{result: r}, nil
}

func (sbc *sandboxConn) Begin(ctx context.Context) (int64, error) {
	sbc.BeginCount.Add(1)
	err := sbc.getError()
	if err != nil {
		return 0, err
	}
	return sbc.TransactionID.Add(1), nil
}

func (sbc *sandboxConn) Commit(ctx context.Context, transactionID int64) error {
	sbc.CommitCount.Add(1)
	return sbc.getError()
}

func (sbc *sandboxConn) Rollback(ctx context.Context, transactionID int64) error {
	sbc.RollbackCount.Add(1)
	return sbc.getError()
}

func (sbc *sandboxConn) BeginExecute(ctx context.Context, query string, bindVars map[string]interface{}) (*sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	result, err := sbc.Execute(ctx, query, bindVars, transactionID)
	return result, transactionID, err
}

func (sbc *sandboxConn) BeginExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool) ([]sqltypes.Result, int64, error) {
	transactionID, err := sbc.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	results, err := sbc.ExecuteBatch(ctx, queries, asTransaction, transactionID)
	return results, transactionID, err
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

// Fake SplitQuery returns a single QuerySplit whose 'sql' field describes the received arguments.
// TODO(erez): Rename to SplitQuery after the migration to SplitQuery V2 is done.
func (sbc *sandboxConn) SplitQueryV2(
	ctx context.Context,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]querytypes.QuerySplit, error) {
	splits := []querytypes.QuerySplit{
		{
			Sql: fmt.Sprintf(
				"query:%v, splitColumns:%v, splitCount:%v,"+
					" numRowsPerQueryPart:%v, algorithm:%v, shard:%v",
				query, splitColumns, splitCount, numRowsPerQueryPart, algorithm, sbc.shard),
		},
	}
	return splits, nil
}

// StreamHealth is not implemented.
func (sbc *sandboxConn) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	return nil, fmt.Errorf("Not implemented in test")
}

// Close does not change ExecCount
func (sbc *sandboxConn) Close() {
}

func (sbc *sandboxConn) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	sbc.keyspace = keyspace
	sbc.shard = shard
	sbc.tabletType = tabletType
	return nil
}

func (sbc *sandboxConn) Tablet() *topodatapb.Tablet {
	return sbc.tablet
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
