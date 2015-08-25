// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	kproto "github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ScatterConnService is the interface for executing queries across multiple shards.
type ScatterConnService interface {
	// InitializeConnections initializes underlying connections during VTGate starts up.
	InitializeConnections(ctx context.Context) error

	// Execute executes a non-streaming query on the specified shards.
	Execute(ctx context.Context, query string, bindVars map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *SafeSession, notInTransaction bool) (*mproto.QueryResult, error)

	// ExecuteMulti executes a non-streaming query on the specified shards.
	// Each shard gets its own bindVars. If len(shards) is not equal to
	// len(bindVars), the function panics.
	ExecuteMulti(ctx context.Context, query string, keyspace string, shardVars map[string]map[string]interface{}, tabletType pb.TabletType, session *SafeSession, notInTransaction bool) (*mproto.QueryResult, error)

	// ExecuteEntityIds executes queries that are shard specific.
	ExecuteEntityIds(ctx context.Context, shards []string, sqls map[string]string, bindVars map[string]map[string]interface{}, keyspace string, tabletType pb.TabletType, session *SafeSession, notInTransaction bool) (*mproto.QueryResult, error)

	// ExecuteBatch executes a batch of non-streaming queries on the specified shards.
	ExecuteBatch(ctx context.Context, batchRequest *scatterBatchRequest, tabletType pb.TabletType, asTransaction bool, session *SafeSession) (qrs *tproto.QueryResultList, err error)

	// StreamExecute executes a streaming query on the specified shards.
	StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(reply *mproto.QueryResult) error) error

	// StreamExecuteMulti executes a streaming query on the specified shards.
	// Each shard gets its own bindVars. If len(shards) is not equal to
	// len(bindVars), the function panics.
	StreamExecuteMulti(ctx context.Context, query string, keyspace string, shardVars map[string]map[string]interface{}, tabletType pb.TabletType, sendReply func(reply *mproto.QueryResult) error) error

	// Commit commits the current transaction. There are no retries on this operation.
	Commit(ctx context.Context, session *SafeSession) (err error)

	// Rollback rolls back the current transaction. There are no retries on this operation.
	Rollback(ctx context.Context, session *SafeSession) (err error)

	// SplitQueryKeyRange scatters a SplitQuery request to all shards. For a set of
	// splits received from a shard, it construct a KeyRange queries by
	// appending that shard's keyrange to the splits. Aggregates all splits across
	// all shards in no specific order and returns.
	SplitQueryKeyRange(ctx context.Context, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int, keyRangeByShard map[string]kproto.KeyRange, keyspace string) ([]proto.SplitQueryPart, error)

	// SplitQueryCustomSharding scatters a SplitQuery request to all
	// shards. For a set of splits received from a shard, it construct a
	// KeyRange queries by appending that shard's name to the
	// splits. Aggregates all splits across all shards in no specific
	// order and returns.
	SplitQueryCustomSharding(ctx context.Context, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int, shards []string, keyspace string) ([]proto.SplitQueryPart, error)

	// Close closes the connection.
	Close() error
}

// scatterBatchRequest needs to be built to perform a scatter batch query.
// A VTGate batch request will get translated into a differnt set of batches
// for each keyspace:shard, and those results will map to different positions in the
// results list. The lenght specifies the total length of the final results
// list. In each request variable, the resultIndexes specifies the position
// for each result from the shard.
type scatterBatchRequest struct {
	Length   int
	Requests map[string]*shardBatchRequest
}

type shardBatchRequest struct {
	Queries         []tproto.BoundQuery
	Keyspace, Shard string
	ResultIndexes   []int
}
