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

// binlogConnStreamer streams binlog events from MySQL by connecting as a slave.
type binlogConnStreamer struct {
	// dbname and mysqld are set at creation.
	dbname string
	mysqld *mysqlctl.Mysqld

	svm  sync2.ServiceManager
	conn *mysqlctl.SlaveConnection

	// startPos is the position at which to start the stream. This is only updated
	// when a transaction is committed or rolled back, to avoid restarting in the
	// middle of a transaction.
	startPos myproto.GTID
	// pos is the most recent event we've seen, even if it's inside a transaction.
	pos myproto.GTID
}

// newBinlogConnStreamer creates a BinlogStreamer.
//
// dbname specifes the db to stream events for.
func newBinlogConnStreamer(dbname string, mysqld *mysqlctl.Mysqld) BinlogStreamer {
	return &binlogConnStreamer{
		dbname: dbname,
		mysqld: mysqld,
	}
}

// Stream implements BinlogStreamer.Stream().
func (bls *binlogConnStreamer) Stream(gtid myproto.GTID, sendTransaction sendTransactionFunc) (err error) {
	bls.startPos = gtid

	// Launch using service manager so we can stop this as needed.
	bls.svm.Go(func(svm *sync2.ServiceManager) {
		var events <-chan proto.BinlogEvent

		// Keep reconnecting and restarting stream unless we've been told to stop.
		for svm.IsRunning() {
			if bls.conn, err = mysqlctl.NewSlaveConnection(bls.mysqld); err != nil {
				return
			}
			events, err = bls.conn.StartBinlogDump(bls.startPos)
			if err != nil {
				bls.conn.Close()
				return
			}
			if err = bls.parseEvents(events, sendTransaction); err != nil {
				bls.conn.Close()
				return
			}
			bls.conn.Close()
		}
	})

	// Wait for service to exit, and handle errors if any.
	bls.svm.Wait()

	if err != nil {
		err = fmt.Errorf("stream error @ %#v, error: %v", bls.pos, err)
		log.Error(err.Error())
		return err
	}

	log.Infof("stream ended @ %#v", bls.pos)
	return nil
}

// Stop implements BinlogStreamer.Stop().
func (bls *binlogConnStreamer) Stop() {
	bls.svm.Stop()
}

// parseEvents processes the raw binlog dump stream from the server, one event
// at a time, and groups them into transactions.
func (bls *binlogConnStreamer) parseEvents(events <-chan proto.BinlogEvent, sendTransaction sendTransactionFunc) (err error) {
	var statements []proto.Statement
	var timestamp int64
	var format proto.BinlogFormat

	// A commit can be triggered either by a COMMIT query, or by an XID_EVENT.
	commit := func() error {
		trans := &proto.BinlogTransaction{
			Statements: statements,
			Timestamp:  timestamp,
			GTIDField:  myproto.GTIDField{Value: bls.pos},
		}
		if err = sendTransaction(trans); err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("send reply error: %v", err)
		}
		statements = nil
		bls.startPos = bls.pos
		return nil
	}

	// Parse events.
	for bls.svm.IsRunning() {
		var ev proto.BinlogEvent
		var ok bool

		select {
		case ev, ok = <-events:
			if !ok {
				log.Infof("reached end of binlog event stream")
				return nil
			}
		case <-bls.svm.ShuttingDown():
			log.Infof("stopping early due to BinlogStreamer service shutdown")
			return nil
		}

		// Validate the buffer before reading fields from it.
		if !ev.IsValid() {
			return fmt.Errorf("can't parse binlog event, invalid data: %#v", ev)
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
				return fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
			}

			format, err = ev.Format()
			if err != nil {
				return fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
			}
			continue
		}

		// Update the GTID if the event has one. The actual event type could be
		// something special like GTID_EVENT (MariaDB, MySQL 5.6), or it could be
		// an arbitrary event with a GTID in the header (Google MySQL).
		if ev.HasGTID(format) {
			bls.pos, err = ev.GTID(format)
			if err != nil {
				return fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
			}
			// If it's a dedicated GTID_EVENT, there's nothing else to do.
			if ev.IsGTID() {
				continue
			}
		}

		switch true {
		case ev.IsXID(): // XID_EVENT (equivalent to COMMIT)
			if err = commit(); err != nil {
				return err
			}
		case ev.IsIntVar(): // INTVAR_EVENT
			name, value, err := ev.IntVar(format)
			if err != nil {
				return fmt.Errorf("can't parse INTVAR_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, proto.Statement{
				Category: proto.BL_SET,
				Sql:      []byte(fmt.Sprintf("SET %s=%d", name, value)),
			})
		case ev.IsRand(): // RAND_EVENT
			seed1, seed2, err := ev.Rand(format)
			if err != nil {
				return fmt.Errorf("can't parse RAND_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, proto.Statement{
				Category: proto.BL_SET,
				Sql:      []byte(fmt.Sprintf("SET @@RAND_SEED1=%d, @@RAND_SEED2=%d", seed1, seed2)),
			})
		case ev.IsQuery(): // QUERY_EVENT
			// Remember the timestamp at the beginning of a transaction.
			if statements == nil {
				timestamp = int64(ev.Timestamp())
			}
			// Extract the query string and group into transactions.
			db, sql, err := ev.Query(format)
			if err != nil {
				return fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
			}
			if db != "" && db != bls.dbname {
				// Skip queries that aren't on the database we're looking for.
				continue
			}
			switch cat := getStatementCategory(sql); cat {
			case proto.BL_BEGIN:
				statements = nil
			case proto.BL_ROLLBACK:
				// TODO(enisoc): When we used to read binlog events through mysqlbinlog,
				// it would generate fake ROLLBACK statements when it reached the end of
				// a file in the middle of a transaction. We don't expect to see
				// ROLLBACK here in the real replication stream, so log it if we do.
				log.Warningf("ROLLBACK encountered in real binlog stream; transaction: %#v, rollback: %#v", statements, string(sql))
				statements = nil
				bls.startPos = bls.pos
			case proto.BL_DDL:
				statements = append(statements, proto.Statement{
					Category: proto.BL_SET,
					Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
				})
				statements = append(statements, proto.Statement{Category: cat, Sql: sql})
				fallthrough
			case proto.BL_COMMIT:
				if err = commit(); err != nil {
					return err
				}
			default: // proto.BL_SET, proto.BL_DML, or proto.BL_UNRECOGNIZED
				statements = append(statements, proto.Statement{
					Category: proto.BL_SET,
					Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
				})
				statements = append(statements, proto.Statement{Category: cat, Sql: sql})
			}
		}
	}

	return nil
}
