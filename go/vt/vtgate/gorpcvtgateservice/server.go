// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

type VTGate struct {
	server *vtgate.VTGate
}

func (vtg *VTGate) ExecuteShard(context *rpcproto.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	return vtg.server.ExecuteShard(context, query, reply)
}

func (vtg *VTGate) ExecuteKeyspaceIds(context *rpcproto.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	return vtg.server.ExecuteKeyspaceIds(context, query, reply)
}

func (vtg *VTGate) ExecuteKeyRanges(context *rpcproto.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	return vtg.server.ExecuteKeyRanges(context, query, reply)
}

func (vtg *VTGate) ExecuteEntityIds(context *rpcproto.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	return vtg.server.ExecuteEntityIds(context, query, reply)
}

func (vtg *VTGate) ExecuteBatchShard(context *rpcproto.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	return vtg.server.ExecuteBatchShard(context, batchQuery, reply)
}

func (vtg *VTGate) ExecuteBatchKeyspaceIds(context *rpcproto.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	return vtg.server.ExecuteBatchKeyspaceIds(context, batchQuery, reply)
}

func (vtg *VTGate) StreamExecuteShard(context *rpcproto.Context, query *proto.QueryShard, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteShard(context, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

func (vtg *VTGate) StreamExecuteKeyRanges(context *rpcproto.Context, query *proto.KeyRangeQuery, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteKeyRanges(context, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

func (vtg *VTGate) StreamExecuteKeyspaceIds(context *rpcproto.Context, query *proto.KeyspaceIdQuery, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteKeyspaceIds(context, query, func(value *proto.QueryResult) error {
		return sendReply(value)
	})
}

func (vtg *VTGate) Begin(context *rpcproto.Context, noInput *rpc.UnusedRequest, outSession *proto.Session) error {
	return vtg.server.Begin(context, outSession)
}

func (vtg *VTGate) Commit(context *rpcproto.Context, inSession *proto.Session, noOutput *rpc.UnusedResponse) error {
	return vtg.server.Commit(context, inSession)
}

func (vtg *VTGate) Rollback(context *rpcproto.Context, inSession *proto.Session, noOutput *rpc.UnusedResponse) error {
	return vtg.server.Rollback(context, inSession)
}

func init() {
	vtgate.RegisterVTGates = append(vtgate.RegisterVTGates, func(vtGate *vtgate.VTGate) {
		rpcwrap.RegisterAuthenticated(&VTGate{vtGate})
	})
}
