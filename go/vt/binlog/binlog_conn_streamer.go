// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"io"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

var ClientEOF = fmt.Errorf("binlog stream consumer ended the reply stream")
var ServerEOF = fmt.Errorf("binlog stream connection was closed by mysqld")

// binlogConnStreamer streams binlog events from MySQL by connecting as a slave.
type binlogConnStreamer struct {
	// dbname and mysqld are set at creation.
	dbname          string
	mysqld          *mysqlctl.Mysqld
	startPos        myproto.ReplicationPosition
	sendTransaction sendTransactionFunc

	conn *mysqlctl.SlaveConnection
}

// newBinlogConnStreamer creates a BinlogStreamer.
//
// dbname specifes the db to stream events for.
func newBinlogConnStreamer(dbname string, mysqld *mysqlctl.Mysqld, startPos myproto.ReplicationPosition, sendTransaction sendTransactionFunc) BinlogStreamer {
	return &binlogConnStreamer{
		dbname:          dbname,
		mysqld:          mysqld,
		startPos:        startPos,
		sendTransaction: sendTransaction,
	}
}

// Stream implements BinlogStreamer.Stream().
func (bls *binlogConnStreamer) Stream(ctx *sync2.ServiceContext) (err error) {
	var stopPos myproto.ReplicationPosition
	defer func() {
		if err != nil {
			binlogStreamerErrors.Add("Stream", 1)
			err = fmt.Errorf("stream error @ %v, error: %v", stopPos, err)
			log.Error(err.Error())
		}
		log.Infof("Stream ended @ %v", stopPos)
	}()

	if bls.conn, err = mysqlctl.NewSlaveConnection(bls.mysqld); err != nil {
		return
	}
	defer bls.conn.Close()

	var events <-chan proto.BinlogEvent
	events, err = bls.conn.StartBinlogDump(bls.startPos)
	if err != nil {
		return
	}
	// parseEvents will loop until the events channel is closed, the
	// service enters the SHUTTING_DOWN state, or an error occurs.
	stopPos, err = bls.parseEvents(ctx, events)
	return
}

// parseEvents processes the raw binlog dump stream from the server, one event
// at a time, and groups them into transactions. It is called from within the
// service function launched by Stream().
//
// If the sendTransaction func returns io.EOF, parseEvents returns ClientEOF.
// If the events channel is closed, parseEvents returns ServerEOF.
func (bls *binlogConnStreamer) parseEvents(ctx *sync2.ServiceContext, events <-chan proto.BinlogEvent) (myproto.ReplicationPosition, error) {
	var statements []proto.Statement
	var format proto.BinlogFormat
	var gtid myproto.GTID
	var pos = bls.startPos
	var autocommit = true
	var err error

	// A begin can be triggered either by a BEGIN query, or by a GTID_EVENT.
	begin := func() {
		if statements != nil {
			// If this happened, it would be a legitimate error.
			log.Errorf("BEGIN in binlog stream while still in another transaction; dropping %d statements: %v", len(statements), statements)
			binlogStreamerErrors.Add("ParseEvents", 1)
		}
		statements = make([]proto.Statement, 0, 10)
		autocommit = false
	}
	// A commit can be triggered either by a COMMIT query, or by an XID_EVENT.
	// Statements that aren't wrapped in BEGIN/COMMIT are committed immediately.
	commit := func(timestamp uint32) error {
		trans := &proto.BinlogTransaction{
			Statements: statements,
			Timestamp:  int64(timestamp),
			GTIDField:  myproto.GTIDField{Value: gtid},
		}
		if err = bls.sendTransaction(trans); err != nil {
			if err == io.EOF {
				return ClientEOF
			}
			return fmt.Errorf("send reply error: %v", err)
		}
		statements = nil
		autocommit = true
		return nil
	}

	// Parse events.
	for ctx.IsRunning() {
		var ev proto.BinlogEvent
		var ok bool

		select {
		case ev, ok = <-events:
			if !ok {
				// events channel has been closed, which means the connection died.
				log.Infof("reached end of binlog event stream")
				return pos, ServerEOF
			}
		case <-ctx.ShuttingDown:
			log.Infof("stopping early due to BinlogStreamer service shutdown")
			return pos, nil
		}

		// Validate the buffer before reading fields from it.
		if !ev.IsValid() {
			return pos, fmt.Errorf("can't parse binlog event, invalid data: %#v", ev)
		}

		// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
		// tells us the size of the event header.
		if format.IsZero() {
			if !ev.IsFormatDescription() {
				// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
				// is a fake ROTATE_EVENT, which the master sends to tell us the name
				// of the current log file.
				if ev.IsRotate() {
					continue
				}
				return pos, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
			}

			format, err = ev.Format()
			if err != nil {
				return pos, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
			}
			continue
		}

		// Update the GTID if the event has one. The actual event type could be
		// something special like GTID_EVENT (MariaDB, MySQL 5.6), or it could be
		// an arbitrary event with a GTID in the header (Google MySQL).
		if ev.HasGTID(format) {
			gtid, err = ev.GTID(format)
			if err != nil {
				return pos, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
			}
			pos = myproto.AppendGTID(pos, gtid)
		}

		switch {
		case ev.IsGTID(): // GTID_EVENT
			if ev.IsBeginGTID(format) {
				begin()
			}
		case ev.IsXID(): // XID_EVENT (equivalent to COMMIT)
			if err = commit(ev.Timestamp()); err != nil {
				return pos, err
			}
		case ev.IsIntVar(): // INTVAR_EVENT
			name, value, err := ev.IntVar(format)
			if err != nil {
				return pos, fmt.Errorf("can't parse INTVAR_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, proto.Statement{
				Category: proto.BL_SET,
				Sql:      []byte(fmt.Sprintf("SET %s=%d", name, value)),
			})
		case ev.IsRand(): // RAND_EVENT
			seed1, seed2, err := ev.Rand(format)
			if err != nil {
				return pos, fmt.Errorf("can't parse RAND_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, proto.Statement{
				Category: proto.BL_SET,
				Sql:      []byte(fmt.Sprintf("SET @@RAND_SEED1=%d, @@RAND_SEED2=%d", seed1, seed2)),
			})
		case ev.IsQuery(): // QUERY_EVENT
			// Extract the query string and group into transactions.
			db, sql, err := ev.Query(format)
			if err != nil {
				return pos, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
			}
			switch cat := getStatementCategory(sql); cat {
			case proto.BL_BEGIN:
				begin()
			case proto.BL_ROLLBACK:
				// Rollbacks are possible under some circumstances. Since the stream
				// client keeps track of its replication position by updating the set
				// of GTIDs it's seen, we must commit an empty transaction so the client
				// can update its position.
				statements = nil
				fallthrough
			case proto.BL_COMMIT:
				if err = commit(ev.Timestamp()); err != nil {
					return pos, err
				}
			default: // BL_DDL, BL_DML, BL_SET, BL_UNRECOGNIZED
				if db != "" && db != bls.dbname {
					// Skip cross-db statements.
					continue
				}
				statements = append(statements, proto.Statement{
					Category: proto.BL_SET,
					Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
				})
				statements = append(statements, proto.Statement{Category: cat, Sql: sql})
				if autocommit {
					if err = commit(ev.Timestamp()); err != nil {
						return pos, err
					}
				}
			}
		}
	}

	return pos, nil
}
