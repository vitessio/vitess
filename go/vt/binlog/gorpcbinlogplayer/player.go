// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogplayer

import (
	"time"

	"github.com/henryanand/vitess/go/rpcplus"
	"github.com/henryanand/vitess/go/rpcwrap/bsonrpc"
	"github.com/henryanand/vitess/go/vt/binlog/binlogplayer"
	"github.com/henryanand/vitess/go/vt/binlog/proto"
)

// GoRpcBinlogPlayerResponse is the type returned by the Client for streaming
type GoRpcBinlogPlayerResponse struct {
	*rpcplus.Call
}

func (response *GoRpcBinlogPlayerResponse) Error() error {
	return response.Call.Error
}

// GoRpcBinlogPlayerClient implements a BinlogPlayerClient over go rpc
type GoRpcBinlogPlayerClient struct {
	*rpcplus.Client
}

func (client *GoRpcBinlogPlayerClient) Dial(addr string, connTimeout time.Duration) error {
	var err error
	client.Client, err = bsonrpc.DialHTTP("tcp", addr, connTimeout, nil)
	return err
}

func (client *GoRpcBinlogPlayerClient) Close() {
	client.Client.Close()
}

func (client *GoRpcBinlogPlayerClient) ServeUpdateStream(req *proto.UpdateStreamRequest, responseChan chan *proto.StreamEvent) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.ServeUpdateStream", req, responseChan)
	return &GoRpcBinlogPlayerResponse{resp}
}

func (client *GoRpcBinlogPlayerClient) StreamKeyRange(req *proto.KeyRangeRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.StreamKeyRange", req, responseChan)
	return &GoRpcBinlogPlayerResponse{resp}
}

func (client *GoRpcBinlogPlayerClient) StreamTables(req *proto.TablesRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.StreamTables", req, responseChan)
	return &GoRpcBinlogPlayerResponse{resp}
}

// Registration as a factory
func init() {
	binlogplayer.RegisterBinlogPlayerClientFactory("gorpc", func() binlogplayer.BinlogPlayerClient {
		return &GoRpcBinlogPlayerClient{}
	})
}
