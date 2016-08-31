// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

/*
This file contains the API and registration mechanism for binlog player client.
*/

var binlogPlayerProtocol = flag.String("binlog_player_protocol", "grpc", "the protocol to download binlogs from a vttablet")

// BinlogTransactionStream is the interface of the object returned by
// StreamTables and StreamKeyRange
type BinlogTransactionStream interface {
	// Recv returns the next BinlogTransaction, or an error if the RPC was
	// interrupted.
	Recv() (*binlogdatapb.BinlogTransaction, error)
}

// Client is the interface all clients must satisfy
type Client interface {
	// Dial a server
	Dial(tablet *topodatapb.Tablet, connTimeout time.Duration) error

	// Close the connection
	Close()

	// Ask the server to stream updates related to the provided tables.
	// Should return context.Canceled if the context is canceled.
	StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (BinlogTransactionStream, error)

	// Ask the server to stream updates related to the provided keyrange.
	// Should return context.Canceled if the context is canceled.
	StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (BinlogTransactionStream, error)
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
