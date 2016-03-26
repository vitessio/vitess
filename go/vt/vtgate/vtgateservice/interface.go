// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateservice provides to interface definition for the
// vtgate service
package vtgateservice

import (
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// VTGateService is the interface implemented by the VTGate service,
// that RPC server implementations will call.
type VTGateService interface {
	// Regular query execution.
	// All these methods can change the provided session.
	Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error)
	ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error)
	ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error)

	// Streaming queries
	StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error
	StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error
	StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error
	StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error

	// Transaction management
	Begin(ctx context.Context) (*vtgatepb.Session, error)
	Commit(ctx context.Context, session *vtgatepb.Session) error
	Rollback(ctx context.Context, session *vtgatepb.Session) error

	// Map Reduce support
	SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]*vtgatepb.SplitQueryResponse_Part, error)

	// MapReduce support
	// TODO(erez): Rename to SplitQuery after migration to SplitQuery V2.
	SplitQueryV2(
		ctx context.Context,
		keyspace string,
		sql string,
		bindVariables map[string]interface{},
		splitColumns []string,
		splitCount int64,
		numRowsPerQueryPart int64,
		algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error)

	// Topology support
	GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error)

	// GetSrvShard is not part of the public API, but might be used
	// by some implementations.
	GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error)

	// HandlePanic should be called with defer at the beginning of each
	// RPC implementation method, before calling any of the previous methods
	HandlePanic(err *error)
}

// CallCorrectSplitQuery calls the correct SplitQuery.
// This trivial logic is encapsulated in a function here so it can be easily tested.
// TODO(erez): Remove once the migration to SplitQueryV2 is done.
func CallCorrectSplitQuery(
	vtgateService VTGateService,
	useSplitQueryV2 bool,
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	if useSplitQueryV2 {
		return vtgateService.SplitQueryV2(
			ctx,
			keyspace,
			sql,
			bindVariables,
			splitColumns,
			splitCount,
			numRowsPerQueryPart,
			algorithm)
	}
	return vtgateService.SplitQuery(
		ctx,
		keyspace,
		sql,
		bindVariables,
		splitColumnsToSplitColumn(splitColumns),
		splitCount)
}

// SplitColumnsToSplitColumn returns the first SplitColumn in the given slice or an empty
// string if the slice is empty.
//
// This method is used to get the traditional behavior when accessing the SplitColumn field in an
// older SplitQuery-V1 vtgatepb.SplitQueryRequest represented in the newer SplitQuery-V2
// vtgatepb.SplitQueryRequest message. In the new V2 message the SplitColumn field has been
// converted into a repeated string field.
// TODO(erez): Remove this function when migration to SplitQueryV2 is done.
func splitColumnsToSplitColumn(splitColumns []string) string {
	if len(splitColumns) == 0 {
		return ""
	}
	return splitColumns[0]
}

// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source $GOFILE -destination vtgateservice_testing/mock_vtgateservice.go -package vtgateservice_testing
