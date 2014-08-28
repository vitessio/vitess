// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bytes"
	"flag"
	"fmt"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
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
		"conn": newBinlogConnStreamer,
	}

	binlogStreamer = flag.String("binlog_streamer", "conn",
		"Which binlog streamer implementation to use. Available: conn, file")

	binlogStreamerErrors = stats.NewCounters("BinlogStreamerErrors")
)

// BinlogStreamer is an interface for requesting a stream of binlog events from
// mysqld starting at a given GTID.
type BinlogStreamer interface {
	// Stream starts streaming binlog events from a given GTID.
	// It calls sendTransaction() with the contens of each event.
	Stream(ctx *sync2.ServiceContext, gtid myproto.GTID, sendTransaction sendTransactionFunc) error
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

var (
	// statementPrefixes are normal sql statement prefixes.
	statementPrefixes = map[string]int{
		"begin":    proto.BL_BEGIN,
		"commit":   proto.BL_COMMIT,
		"rollback": proto.BL_ROLLBACK,
		"insert":   proto.BL_DML,
		"update":   proto.BL_DML,
		"delete":   proto.BL_DML,
		"create":   proto.BL_DDL,
		"alter":    proto.BL_DDL,
		"drop":     proto.BL_DDL,
		"truncate": proto.BL_DDL,
		"rename":   proto.BL_DDL,
		"set":      proto.BL_SET,
	}
)

// getStatementCategory returns the proto.BL_* category for a SQL statement.
func getStatementCategory(sql []byte) int {
	if i := bytes.IndexByte(sql, byte(' ')); i >= 0 {
		sql = sql[:i]
	}
	return statementPrefixes[string(bytes.ToLower(sql))]
}
