// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	pb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// CallerIDPrefix is the prefix to send with queries to they go
// through this test suite.
const CallerIDPrefix = "callerid "

// callerIDClient implements vtgateservice.VTGateService, and checks
// the received callerid matches the one passed out of band by the client.
type callerIDClient struct {
	fallback vtgateservice.VTGateService
}

func newCallerIDClient(fallback vtgateservice.VTGateService) *callerIDClient {
	return &callerIDClient{
		fallback: fallback,
	}
}

// checkCallerID will see if this module is handling the request,
// and if it is, check the callerID from the context.
// Returns false if the query is not for this module.
// Returns true and the the error to return with the call
// if this module is handling the request.
func (c *callerIDClient) checkCallerID(ctx context.Context, received string) (bool, error) {
	if !strings.HasPrefix(received, CallerIDPrefix) {
		return false, nil
	}

	jsonCallerID := []byte(received[len(CallerIDPrefix):])
	expectedCallerID := &pb.CallerID{}
	if err := json.Unmarshal(jsonCallerID, expectedCallerID); err != nil {
		return true, fmt.Errorf("cannot unmarshal provided callerid: %v", err)
	}

	receivedCallerID := callerid.EffectiveCallerIDFromContext(ctx)
	if receivedCallerID == nil {
		return true, fmt.Errorf("no callerid received in the query")
	}

	if !reflect.DeepEqual(receivedCallerID, expectedCallerID) {
		return true, fmt.Errorf("callerid mismatch, got %v expected %v", receivedCallerID, expectedCallerID)
	}

	return true, nil
}

func (c *callerIDClient) Execute(ctx context.Context, query *proto.Query, reply *proto.QueryResult) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.Execute(ctx, query, reply)
}

func (c *callerIDClient) ExecuteShard(ctx context.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.ExecuteShard(ctx, query, reply)
}

func (c *callerIDClient) ExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.ExecuteKeyspaceIds(ctx, query, reply)
}

func (c *callerIDClient) ExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.ExecuteKeyRanges(ctx, query, reply)
}

func (c *callerIDClient) ExecuteEntityIds(ctx context.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.ExecuteEntityIds(ctx, query, reply)
}

func (c *callerIDClient) ExecuteBatchShard(ctx context.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	if len(batchQuery.Queries) == 1 {
		if ok, err := c.checkCallerID(ctx, batchQuery.Queries[0].Sql); ok {
			return err
		}
	}
	return c.fallback.ExecuteBatchShard(ctx, batchQuery, reply)
}

func (c *callerIDClient) ExecuteBatchKeyspaceIds(ctx context.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	if len(batchQuery.Queries) == 1 {
		if ok, err := c.checkCallerID(ctx, batchQuery.Queries[0].Sql); ok {
			return err
		}
	}
	return c.fallback.ExecuteBatchKeyspaceIds(ctx, batchQuery, reply)
}

func (c *callerIDClient) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(*proto.QueryResult) error) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.StreamExecute(ctx, query, sendReply)
}

func (c *callerIDClient) StreamExecuteShard(ctx context.Context, query *proto.QueryShard, sendReply func(*proto.QueryResult) error) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.StreamExecuteShard(ctx, query, sendReply)
}

func (c *callerIDClient) StreamExecuteKeyRanges(ctx context.Context, query *proto.KeyRangeQuery, sendReply func(*proto.QueryResult) error) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.StreamExecuteKeyRanges(ctx, query, sendReply)
}

func (c *callerIDClient) StreamExecuteKeyspaceIds(ctx context.Context, query *proto.KeyspaceIdQuery, sendReply func(*proto.QueryResult) error) error {
	if ok, err := c.checkCallerID(ctx, query.Sql); ok {
		return err
	}
	return c.fallback.StreamExecuteKeyspaceIds(ctx, query, sendReply)
}

func (c *callerIDClient) Begin(ctx context.Context, outSession *proto.Session) error {
	return c.fallback.Begin(ctx, outSession)
}

func (c *callerIDClient) Commit(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Commit(ctx, inSession)
}

func (c *callerIDClient) Rollback(ctx context.Context, inSession *proto.Session) error {
	return c.fallback.Rollback(ctx, inSession)
}

func (c *callerIDClient) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	if ok, err := c.checkCallerID(ctx, req.Query.Sql); ok {
		return err
	}
	return c.fallback.SplitQuery(ctx, req, reply)
}

func (c *callerIDClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}

func (c *callerIDClient) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
