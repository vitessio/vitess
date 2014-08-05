// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"flag"
	"fmt"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var (
	// TODO(enisoc): All the implementations are listed in a central place because
	// BinlogStreamers are not meant to be pluggable long-term. We are only
	// keeping the old file-based implementation around as a fallback until the
	// connection-based implementation is stable.
	binlogStreamers = map[string]newBinlogStreamerFunc{
		"file": newBinlogFileStreamer,
	}

	binlogStreamer = flag.String("binlog_streamer", "file",
		"Which binlog streamer implementation to use. Available: file")
)

// BinlogStreamer is an interface for requesting a stream of binlog events from
// mysqld starting at a given GTID.
type BinlogStreamer interface {
	// Stream starts streaming binlog events from a given GTID.
	// It calls sendTransaction() with the contens of each event.
	Stream(gtid myproto.GTID, sendTransaction sendTransactionFunc) error

	// Stop stops the currently executing Stream() call if there is one.
	Stop()
}

// NewBinlogStreamer creates a BinlogStreamer. The underlying implementation is
// selected by the -binlog_streamer=<implementation> flag.
//
// dbname specifes the db to stream events for.
// mysqld is the local instance of mysqlctl.Mysqld.
func NewBinlogStreamer(dbname string, mysqld *mysqlctl.Mysqld) BinlogStreamer {
	fn := binlogStreamers[*binlogStreamer]
	if fn == nil {
		panic(fmt.Errorf("unknown BinlogStreamer implementation: %#v", *binlogStreamer))
	}
	return fn(dbname, mysqld)
}

type newBinlogStreamerFunc func(string, *mysqlctl.Mysqld) BinlogStreamer

// sendTransactionFunc is used to send binlog events.
// reply is of type proto.BinlogTransaction.
type sendTransactionFunc func(trans *proto.BinlogTransaction) error

type binlogPosition struct {
	GTID     myproto.GTID
	ServerId int64
}
