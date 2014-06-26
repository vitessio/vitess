// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateservice provides to go rpc glue for vtgate
package gorpcvtgateservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/tabletserver/gorpctabletconn"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

type VTGate struct {
	server      *vtgate.VTGate
	converterID string
}

// NewVTGate creates VTGate and determines if result conversion is needed.
func NewVTGate(server *vtgate.VTGate) *VTGate {
	vtg := &VTGate{server: server}
	tabletProtocol := tabletconn.GetTabletConnProtocol()
	if tabletProtocol != gorpctabletconn.ProtocolBson {
		vtg.converterID = tabletconn.MakeConverterID(
			tabletProtocol,
			gorpctabletconn.ProtocolBson)
	}
	return vtg
}

func (vtg *VTGate) convertQueryResult(result interface{}) *mproto.QueryResult {
	var res *mproto.QueryResult
	if vtg.converterID != "" {
		res = new(mproto.QueryResult)
		tabletconn.ConvertQueryResult(vtg.converterID, result, res)
	} else {
		res = result.(*mproto.QueryResult)
	}
	return res
}

func (vtg *VTGate) convertQueryResultList(result interface{}) *tproto.QueryResultList {
	var res *tproto.QueryResultList
	if vtg.converterID != "" {
		res = new(tproto.QueryResultList)
		tabletconn.ConvertQueryResultList(vtg.converterID, result, res)
	} else {
		res = result.(*tproto.QueryResultList)
	}
	return res
}

func (vtg *VTGate) ExecuteShard(context *rpcproto.Context, query *proto.QueryShard, reply *proto.QueryResult) error {
	result, session, err := vtg.server.ExecuteShard(context, query)
	if err == nil {
		reply.Result = vtg.convertQueryResult(result)
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) ExecuteKeyspaceIds(context *rpcproto.Context, query *proto.KeyspaceIdQuery, reply *proto.QueryResult) error {
	result, session, err := vtg.server.ExecuteKeyspaceIds(context, query)
	if err == nil {
		reply.Result = vtg.convertQueryResult(result)
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) ExecuteKeyRanges(context *rpcproto.Context, query *proto.KeyRangeQuery, reply *proto.QueryResult) error {
	result, session, err := vtg.server.ExecuteKeyRanges(context, query)
	if err == nil {
		reply.Result = vtg.convertQueryResult(result)
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) ExecuteEntityIds(context *rpcproto.Context, query *proto.EntityIdsQuery, reply *proto.QueryResult) error {
	result, session, err := vtg.server.ExecuteEntityIds(context, query)
	if err == nil {
		reply.Result = vtg.convertQueryResult(result)
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) ExecuteBatchShard(context *rpcproto.Context, batchQuery *proto.BatchQueryShard, reply *proto.QueryResultList) error {
	results, session, err := vtg.server.ExecuteBatchShard(context, batchQuery)
	if err == nil {
		reply.List = vtg.convertQueryResultList(results).List
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) ExecuteBatchKeyspaceIds(context *rpcproto.Context, batchQuery *proto.KeyspaceIdBatchQuery, reply *proto.QueryResultList) error {
	results, session, err := vtg.server.ExecuteBatchKeyspaceIds(context, batchQuery)
	if err == nil {
		reply.List = vtg.convertQueryResultList(results).List
	} else {
		reply.Error = err.Error()
	}
	reply.Session = session
	return nil
}

func (vtg *VTGate) StreamExecuteShard(context *rpcproto.Context, query *proto.QueryShard, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteShard(context, query, func(value interface{}, session *proto.Session) error {
		reply := new(proto.QueryResult)
		if value != nil {
			reply.Result = vtg.convertQueryResult(value)
		}
		if session != nil {
			reply.Session = session
		}
		return sendReply(reply)
	})
}

func (vtg *VTGate) StreamExecuteKeyRanges(context *rpcproto.Context, query *proto.KeyRangeQuery, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteKeyRanges(context, query, func(value interface{}, session *proto.Session) error {
		reply := new(proto.QueryResult)
		if value != nil {
			reply.Result = vtg.convertQueryResult(value)
		}
		if session != nil {
			reply.Session = session
		}
		return sendReply(reply)
	})
}

func (vtg *VTGate) StreamExecuteKeyspaceIds(context *rpcproto.Context, query *proto.KeyspaceIdQuery, sendReply func(interface{}) error) error {
	return vtg.server.StreamExecuteKeyspaceIds(context, query, func(value interface{}, session *proto.Session) error {
		reply := new(proto.QueryResult)
		if value != nil {
			reply.Result = vtg.convertQueryResult(value)
		}
		if session != nil {
			reply.Session = session
		}
		return sendReply(reply)
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
		rpcwrap.RegisterAuthenticated(NewVTGate(vtGate))
	})
}
