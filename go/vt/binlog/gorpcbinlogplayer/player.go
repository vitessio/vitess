// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogplayer

import (
	"time"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// response is the type returned by the Client for streaming
type response struct {
	*rpcplus.Call
}

func (response *response) Error() error {
	return response.Call.Error
}

// client implements a BinlogPlayerClient over go rpc
type client struct {
	*rpcplus.Client
}

func (client *client) Dial(endPoint topo.EndPoint, connTimeout time.Duration) error {
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.NamedPortMap["vt"])
	var err error
	client.Client, err = bsonrpc.DialHTTP("tcp", addr, connTimeout, nil)
	return err
}

func (client *client) Close() {
	client.Client.Close()
}

func (client *client) ServeUpdateStream(req *proto.UpdateStreamRequest, responseChan chan *proto.StreamEvent) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.ServeUpdateStream", req, responseChan)
	return &response{resp}
}

func (client *client) StreamKeyRange(req *proto.KeyRangeRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.StreamKeyRange", req, responseChan)
	return &response{resp}
}

func (client *client) StreamTables(req *proto.TablesRequest, responseChan chan *proto.BinlogTransaction) binlogplayer.BinlogPlayerResponse {
	resp := client.Client.StreamGo("UpdateStream.StreamTables", req, responseChan)
	return &response{resp}
}

// Registration as a factory
func init() {
	binlogplayer.RegisterBinlogPlayerClientFactory("gorpc", func() binlogplayer.BinlogPlayerClient {
		return &client{}
	})
}
