// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

/*
This file contains the API and registration mechanism for binlog player client.
*/

var binlogPlayerProtocol = flag.String("binlog_player_protocol", "gorpc", "the protocol to download binlogs from a vttablet")
var binlogPlayerConnTimeout = flag.Duration("binlog_player_conn_timeout", 5*time.Second, "binlog player connection timeout")

// ErrFunc is a return value for streaming events
type ErrFunc func() error

// Client is the interface all clients must satisfy
type Client interface {
	// Dial a server
	Dial(endPoint topo.EndPoint, connTimeout time.Duration) error

	// Close the connection
	Close()

	// Ask the server to stream binlog updates.
	// Should return context.Canceled if the context is canceled.
	ServeUpdateStream(context.Context, *proto.UpdateStreamRequest) (chan *proto.StreamEvent, ErrFunc, error)

	// Ask the server to stream updates related to the provided tables.
	// Should return context.Canceled if the context is canceled.
	StreamTables(context.Context, *proto.TablesRequest) (chan *proto.BinlogTransaction, ErrFunc, error)

	// Ask the server to stream updates related to thee provided keyrange.
	// Should return context.Canceled if the context is canceled.
	StreamKeyRange(context.Context, *proto.KeyRangeRequest) (chan *proto.BinlogTransaction, ErrFunc, error)
}

// ClientFactory is the factory method to create a Client
type ClientFactory func() Client

var clientFactories = make(map[string]ClientFactory)

// RegisterClientFactory adds a new factory. Call during init().
func RegisterClientFactory(name string, factory ClientFactory) {
	if _, ok := clientFactories[name]; ok {
		log.Fatalf("ClientFactory %s already exists", name)
	}
	clientFactories[name] = factory
}
