// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
)

/*
This file contains the API and registration mechanism for binlog player client.
*/

var binlogPlayerProtocol = flag.String("binlog_player_protocol", "gorpc", "the protocol to download binlogs from a vttablet")
var binlogPlayerConnTimeout = flag.Duration("binlog_player_conn_timeout", 5*time.Second, "binlog player connection timeout")

// BinlogPlayerResponse is the return value for streaming events
type BinlogPlayerResponse interface {
	Error() error
}

// BinlogPlayerClient is the interface all clients must satisfy
type BinlogPlayerClient interface {
	// Dial a server
	Dial(addr string, connTimeout time.Duration) error

	// Close the connection
	Close()

	// Ask the server to stream binlog updates
	ServeUpdateStream(*proto.UpdateStreamRequest, chan *proto.StreamEvent) BinlogPlayerResponse

	// Ask the server to stream updates related to the provided tables
	StreamTables(*proto.TablesRequest, chan *proto.BinlogTransaction) BinlogPlayerResponse

	// Ask the server to stream updates related to thee provided keyrange
	StreamKeyRange(*proto.KeyRangeRequest, chan *proto.BinlogTransaction) BinlogPlayerResponse
}

type BinlogPlayerClientFactory func() BinlogPlayerClient

var binlogPlayerClientFactories = make(map[string]BinlogPlayerClientFactory)

func RegisterBinlogPlayerClientFactory(name string, factory BinlogPlayerClientFactory) {
	if _, ok := binlogPlayerClientFactories[name]; ok {
		log.Fatalf("BinlogPlayerClientFactory %s already exists", name)
	}
	binlogPlayerClientFactories[name] = factory
}
