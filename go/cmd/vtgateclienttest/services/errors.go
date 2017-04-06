// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// errorClient implements vtgateservice.VTGateService
// and returns specific errors. It is meant to test all possible error cases,
// and make sure all clients handle the errors correctly.

const (
	// ErrorPrefix is the prefix to send with queries so they go through this service handler.
	ErrorPrefix = "error://"
	// PartialErrorPrefix is the prefix to send with queries so the RPC returns a partial error.
	// A partial error is when we return an error as part of the RPC response instead of via
	// the regular error channels. This occurs if an RPC partially succeeds, and therefore
	// requires some kind of response, but still needs to return an error.
	// VTGate Execute* calls do this: they always return a new session ID, but might also
	// return an error in the response.
	PartialErrorPrefix = "partialerror://"
)

type errorClient struct {
	fallbackClient
}

func newErrorClient(fallback vtgateservice.VTGateService) *errorClient {
	return &errorClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

// requestToError returns an error for the given request, by looking at the
// request's prefix and requested error type. If the request doesn't match an
// error request, return nil.
func requestToError(request string) error {
	if !strings.HasPrefix(request, ErrorPrefix) {
		return nil
	}
	return trimmedRequestToError(strings.TrimPrefix(request, ErrorPrefix))
}

// requestToPartialError fills reply for a partial error if requested
// (that is, an error that may change the session).
// It returns true if a partial error was requested, false otherwise.
// This partial error should only be returned by Execute* calls.
func requestToPartialError(request string, session *vtgatepb.Session) error {
	if !strings.HasPrefix(request, PartialErrorPrefix) {
		return nil
	}
	request = strings.TrimPrefix(request, PartialErrorPrefix)
	parts := strings.Split(request, "/")
	if len(parts) > 1 && parts[1] == "close transaction" {
		session.InTransaction = false
	}
	return trimmedRequestToError(parts[0])
}

// trimmedRequestToError returns an error for a trimmed request by looking at the
// requested error type. It assumes that prefix checking has already been done.
// If the received string doesn't match a known error, returns an unknown error.
func trimmedRequestToError(received string) error {
	switch received {
	case "bad input":
		return vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "vtgate test client forced error: bad input")
	case "deadline exceeded":
		return vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "vtgate test client forced error: deadline exceeded")
	case "integrity error":
		return vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "vtgate test client forced error: integrity error (errno 1062) (sqlstate 23000)")
	// request backlog and general throttling type errors
	case "transient error":
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "request_backlog: too many requests in flight: vtgate test client forced error: transient error")
	case "throttled error":
		return vterrors.New(vtrpcpb.Code_UNAVAILABLE, "request_backlog: exceeded XXX quota, rate limiting: vtgate test client forced error: transient error")
	case "unauthenticated":
		return vterrors.New(vtrpcpb.Code_UNAUTHENTICATED, "vtgate test client forced error: unauthenticated")
	case "aborted":
		return vterrors.New(vtrpcpb.Code_ABORTED, "vtgate test client forced error: aborted")
	case "query not served":
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vtgate test client forced error: query not served")
	case "unknown error":
		return vterrors.New(vtrpcpb.Code_UNKNOWN, "vtgate test client forced error: unknown error")
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "vtgate test client error request unrecognized: %v", received)
	}
}

func (c *errorClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return session, nil, err
	}
	if err := requestToError(sql); err != nil {
		return session, nil, err
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, notInTransaction, options)
}

func (c *errorClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return nil, err
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, options)
}

func (c *errorClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return nil, err
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, options)
}

func (c *errorClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return nil, err
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, options)
}

func (c *errorClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if err := requestToPartialError(sql, session); err != nil {
		return nil, err
	}
	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
}

func (c *errorClient) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) == 1 {
		if err := requestToPartialError(sqlList[0], session); err != nil {
			return session, nil, err
		}
		if err := requestToError(sqlList[0]); err != nil {
			return session, nil, err
		}
	}
	return c.fallbackClient.ExecuteBatch(ctx, sqlList, bindVariablesList, keyspace, tabletType, session, options)
}

func (c *errorClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		if err := requestToPartialError(queries[0].Query.Sql, session); err != nil {
			return nil, err
		}
		if err := requestToError(queries[0].Query.Sql); err != nil {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *errorClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		if err := requestToPartialError(queries[0].Query.Sql, session); err != nil {
			return nil, err
		}
		if err := requestToError(queries[0].Query.Sql); err != nil {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *errorClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, keyspace, tabletType, options, callback)
}

func (c *errorClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, options, callback)
}

func (c *errorClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, options, callback)
}

func (c *errorClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if err := requestToError(sql); err != nil {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, options, callback)
}

func (c *errorClient) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return nil, err
	}
	return c.fallbackClient.Begin(ctx, singledb)
}

func (c *errorClient) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallbackClient.Commit(ctx, twopc, session)
}

func (c *errorClient) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	// The client sends the error request through the callerid, as there are no other parameters
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallbackClient.Rollback(ctx, session)
}

func (c *errorClient) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return err
	}
	return c.fallback.MessageStream(ctx, keyspace, shard, keyRange, name, callback)
}

func (c *errorClient) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	cid := callerid.EffectiveCallerIDFromContext(ctx)
	request := callerid.GetPrincipal(cid)
	if err := requestToError(request); err != nil {
		return 0, err
	}
	return c.fallback.MessageAck(ctx, keyspace, name, ids)
}

func (c *errorClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	if err := requestToError(sql); err != nil {
		return nil, err
	}
	return c.fallbackClient.SplitQuery(
		ctx,
		sql,
		keyspace,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func (c *errorClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if err := requestToError(keyspace); err != nil {
		return nil, err
	}
	return c.fallbackClient.GetSrvKeyspace(ctx, keyspace)
}

func (c *errorClient) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	if err := requestToError(shard); err != nil {
		return err
	}
	return c.fallbackClient.UpdateStream(ctx, keyspace, shard, keyRange, tabletType, timestamp, event, callback)
}
