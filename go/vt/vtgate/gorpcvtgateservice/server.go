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

func (vtg *VTGate) ExecuteBatchShard(context *rpcproto.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	return vtg.server.ExecuteBatchShard(context, batchQuery, reply)
}

func (vtg *VTGate) StreamExecuteKeyRange(context *rpcproto.Context, query *proto.StreamQueryKeyRange, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteKeyRange(context, query, func(value *proto.QueryResult) error {
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
