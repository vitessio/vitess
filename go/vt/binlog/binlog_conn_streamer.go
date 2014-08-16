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
	dbname string
	mysqld *mysqlctl.Mysqld

	svm  sync2.ServiceManager
	conn *mysqlctl.SlaveConnection

	// pos is the GTID of the most recent event we've seen.
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
func (bls *binlogConnStreamer) Stream(startPos myproto.GTID, sendTransaction sendTransactionFunc) error {
	// Launch using service manager so we can stop this as needed.
	bls.svm.Go(func(svc *sync2.ServiceContext) error {
		var events <-chan proto.BinlogEvent
		var err error

		if bls.conn, err = mysqlctl.NewSlaveConnection(bls.mysqld); err != nil {
			return err
		}
		defer bls.conn.Close()

		events, err = bls.conn.StartBinlogDump(startPos)
		if err != nil {
			return err
		}
		// parseEvents will loop until the events channel is closed, the
		// service enters the SHUTTING_DOWN state, or an error occurs.
		err = bls.parseEvents(svc, events, sendTransaction)
		if err == ClientEOF {
			return nil
		}
		return err
	})

	// Wait for service to exit, and handle errors if any.
	err := bls.svm.Join()

	if err != nil {
		binlogStreamerErrors.Add("Stream", 1)
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
// at a time, and groups them into transactions. It is called from within the
// service function launched by Stream().
//
// If the sendTransaction func returns io.EOF, parseEvents returns ClientEOF.
// If the events channel is closed, parseEvents returns ServerEOF.
func (bls *binlogConnStreamer) parseEvents(svc *sync2.ServiceContext, events <-chan proto.BinlogEvent, sendTransaction sendTransactionFunc) (err error) {
	var statements []proto.Statement
	var timestamp int64
	var format proto.BinlogFormat

	// A commit can be triggered either by a COMMIT query, or by an XID_EVENT.
	commit := func() error {
		if statements == nil {
			log.Errorf("COMMIT in binlog stream without matching BEGIN; sending empty transaction")
			binlogStreamerErrors.Add("ParseEvents", 1)
		}
		trans := &proto.BinlogTransaction{
			Statements: statements,
			Timestamp:  timestamp,
			GTIDField:  myproto.GTIDField{Value: bls.pos},
		}
		if err = sendTransaction(trans); err != nil {
			if err == io.EOF {
				return ClientEOF
			}
			return fmt.Errorf("send reply error: %v", err)
		}
		statements = nil
		return nil
	}

	// Parse events.
	for svc.IsRunning() {
		var ev proto.BinlogEvent
		var ok bool

		select {
		case ev, ok = <-events:
			if !ok {
				// events channel has been closed, which means the connection died.
				log.Infof("reached end of binlog event stream")
				return ServerEOF
			}
		case <-svc.ShuttingDown:
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
			// Extract the query string and group into transactions.
			db, sql, err := ev.Query(format)
			if err != nil {
				return fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
			}
			switch cat := getStatementCategory(sql); cat {
			case proto.BL_BEGIN:
				if statements != nil {
					log.Errorf("BEGIN in binlog stream while still in another transaction; dropping %d statements: %v", len(statements), statements)
					binlogStreamerErrors.Add("ParseEvents", 1)
				}
				statements = make([]proto.Statement, 0, 10)
				// Remember the timestamp at the beginning of a transaction.
				timestamp = int64(ev.Timestamp())
			case proto.BL_COMMIT:
				if err = commit(); err != nil {
					return err
				}
			case proto.BL_ROLLBACK:
				// TODO(enisoc): When we used to read binlog events through mysqlbinlog,
				// it would generate fake ROLLBACK statements when it reached the end of
				// a file in the middle of a transaction. We don't expect to see
				// ROLLBACK here in the real replication stream, so log it if we do.
				log.Warningf("ROLLBACK encountered in real binlog stream; transaction: %#v, rollback: %#v", statements, string(sql))
				binlogStreamerErrors.Add("Rollbacks", 1)
				statements = nil
			default: // BL_DDL, BL_DML, BL_SET, BL_UNRECOGNIZED
				if cat == proto.BL_DML && statements == nil {
					log.Errorf("got DML in binlog stream without a BEGIN")
					binlogStreamerErrors.Add("ParseEvents", 1)
				}
				if db != "" && db != bls.dbname {
					// Skip non-transaction-related queries with a default database other
					// than the one we're looking for. This is the behavior we were used to
					// seeing from mysqlbinlog --database=<dbname>.
					continue
				}
				inTransaction := statements != nil
				statements = append(statements, proto.Statement{
					Category: proto.BL_SET,
					Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
				})
				statements = append(statements, proto.Statement{Category: cat, Sql: sql})
				if cat == proto.BL_DDL && !inTransaction {
					// Pretend we saw a BEGIN/COMMIT for DDLs if there wasn't a real BEGIN.
					timestamp = int64(ev.Timestamp())
					if err = commit(); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}
