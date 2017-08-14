/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var (
	binlogStreamerErrors = stats.NewCounters("BinlogStreamerErrors")

	// ErrClientEOF is returned by Streamer if the stream ended because the
	// consumer of the stream indicated it doesn't want any more events.
	ErrClientEOF = fmt.Errorf("binlog stream consumer ended the reply stream")
	// ErrServerEOF is returned by Streamer if the stream ended because the
	// connection to the mysqld server was lost, or the stream was terminated by
	// mysqld.
	ErrServerEOF = fmt.Errorf("binlog stream connection was closed by mysqld")

	// statementPrefixes are normal sql statement prefixes.
	statementPrefixes = map[string]binlogdatapb.BinlogTransaction_Statement_Category{
		"begin":    binlogdatapb.BinlogTransaction_Statement_BL_BEGIN,
		"commit":   binlogdatapb.BinlogTransaction_Statement_BL_COMMIT,
		"rollback": binlogdatapb.BinlogTransaction_Statement_BL_ROLLBACK,
		"insert":   binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
		"update":   binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
		"delete":   binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
		"create":   binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"alter":    binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"drop":     binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"truncate": binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"rename":   binlogdatapb.BinlogTransaction_Statement_BL_DDL,
		"set":      binlogdatapb.BinlogTransaction_Statement_BL_SET,
	}
)

// FullBinlogStatement has all the information we can gather for an event.
// Some fields are only set if asked for, and if RBR is used.
// Otherwise we'll revert back to using the SQL comments, for SBR.
type FullBinlogStatement struct {
	Statement  *binlogdatapb.BinlogTransaction_Statement
	Table      string
	KeyspaceID []byte
	PKNames    []*querypb.Field
	PKValues   []sqltypes.Value
}

// sendTransactionFunc is used to send binlog events.
// reply is of type binlogdatapb.BinlogTransaction.
type sendTransactionFunc func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error

// getStatementCategory returns the binlogdatapb.BL_* category for a SQL statement.
func getStatementCategory(sql string) binlogdatapb.BinlogTransaction_Statement_Category {
	if i := strings.IndexByte(sql, byte(' ')); i >= 0 {
		sql = sql[:i]
	}
	return statementPrefixes[strings.ToLower(sql)]
}

// tableCacheEntry contains everything we know about a table.
// It is created when we get a TableMap event.
type tableCacheEntry struct {
	// tm is what we get from a TableMap event.
	tm *mysql.TableMap

	// ti is the table descriptor we get from the schema engine.
	ti *schema.Table

	// The following fields are used if we want to extract the
	// keyspace_id of a row.

	// resolver is only set if Streamer.resolverFactory is set.
	resolver keyspaceIDResolver

	// keyspaceIDIndex is the index of the field that can be used
	// to compute the keyspaceID. Set to -1 if no resolver is in used.
	keyspaceIDIndex int

	// The following fields are used if we want to extract the
	// primary key of a row.

	// pkNames contains an array of fields for the PK.
	pkNames []*querypb.Field

	// pkIndexes contains the index of a given column in the
	// PK. It is -1 f the column is not in any PK. It contains as
	// many fields as there are columns in the table.
	// For instance, in a table defined like this:
	//   field1 varchar()
	//   pkpart2 int
	//   pkpart1 int
	// pkIndexes would contain: [
	// -1      // field1 is not in the pk
	// 1       // pkpart2 is the second part of the PK
	// 0       // pkpart1 is the first part of the PK
	// This array is built this way so when we extract the columns
	// in a row, we can just save them in the PK array easily.
	pkIndexes []int
}

// Streamer streams binlog events from MySQL by connecting as a slave.
// A Streamer should only be used once. To start another stream, call
// NewStreamer() again.
type Streamer struct {
	// The following fields at set at creation and immutable.
	dbname          string
	mysqld          mysqlctl.MysqlDaemon
	se              *schema.Engine
	resolverFactory keyspaceIDResolverFactory
	extractPK       bool

	clientCharset    *binlogdatapb.Charset
	startPos         mysql.Position
	timestamp        int64
	sendTransaction  sendTransactionFunc
	usePreviousGTIDs bool

	conn *mysqlctl.SlaveConnection
}

// NewStreamer creates a binlog Streamer.
//
// dbname specifes the database to stream events for.
// mysqld is the local instance of mysqlctl.Mysqld.
// charset is the default character set on the BinlogPlayer side.
// startPos is the position to start streaming at. Incompatible with timestamp.
// timestamp is the timestamp to start streaming at. Incompatible with startPos.
// sendTransaction is called each time a transaction is committed or rolled back.
func NewStreamer(dbname string, mysqld mysqlctl.MysqlDaemon, se *schema.Engine, clientCharset *binlogdatapb.Charset, startPos mysql.Position, timestamp int64, sendTransaction sendTransactionFunc) *Streamer {
	return &Streamer{
		dbname:          dbname,
		mysqld:          mysqld,
		se:              se,
		clientCharset:   clientCharset,
		startPos:        startPos,
		timestamp:       timestamp,
		sendTransaction: sendTransaction,
	}
}

// Stream starts streaming binlog events using the settings from NewStreamer().
func (bls *Streamer) Stream(ctx context.Context) (err error) {
	stopPos := bls.startPos
	defer func() {
		if err != nil && err != mysqlctl.ErrBinlogUnavailable {
			err = fmt.Errorf("stream error @ %v: %v", stopPos, err)
		}
		log.Infof("stream ended @ %v, err = %v", stopPos, err)
	}()

	if bls.conn, err = bls.mysqld.NewSlaveConnection(); err != nil {
		return err
	}
	defer bls.conn.Close()

	// Check that the default charsets match, if the client specified one.
	// Note that Streamer uses the settings for the 'dba' user, while
	// BinlogPlayer uses the 'filtered' user, so those are the ones whose charset
	// must match. Filtered replication should still succeed even with a default
	// mismatch, since we pass per-statement charset info. However, Vitess in
	// general doesn't support servers with different default charsets, so we
	// treat it as a configuration error.
	if bls.clientCharset != nil {
		cs, err := mysql.GetCharset(bls.conn.Conn)
		if err != nil {
			return fmt.Errorf("can't get charset to check binlog stream: %v", err)
		}
		log.Infof("binlog stream client charset = %v, server charset = %v", *bls.clientCharset, cs)
		if !proto.Equal(cs, bls.clientCharset) {
			return fmt.Errorf("binlog stream client charset (%v) doesn't match server (%v)", bls.clientCharset, cs)
		}
	}

	var events <-chan mysql.BinlogEvent
	if bls.timestamp != 0 {
		// MySQL 5.6 only: We are going to start reading the
		// logs from the beginning of a binlog file. That is
		// going to send us the PREVIOUS_GTIDS_EVENT that
		// contains the starting GTIDSet, and we will save
		// that as the current position.
		bls.usePreviousGTIDs = true
		events, err = bls.conn.StartBinlogDumpFromBinlogBeforeTimestamp(ctx, bls.timestamp)
	} else if !bls.startPos.IsZero() {
		// MySQL 5.6 only: we are starting from a random
		// binlog position. It turns out we will receive a
		// PREVIOUS_GTIDS_EVENT event, that has a GTIDSet
		// extracted from the binlogs. It is not related to
		// the starting position we pass in, it seems it is
		// just the PREVIOUS_GTIDS_EVENT from the file we're reading.
		// So we have to skip it.
		events, err = bls.conn.StartBinlogDumpFromPosition(ctx, bls.startPos)
	} else {
		bls.startPos, events, err = bls.conn.StartBinlogDumpFromCurrent(ctx)
	}
	if err != nil {
		return err
	}
	// parseEvents will loop until the events channel is closed, the
	// service enters the SHUTTING_DOWN state, or an error occurs.
	stopPos, err = bls.parseEvents(ctx, events)
	return err
}

// parseEvents processes the raw binlog dump stream from the server, one event
// at a time, and groups them into transactions. It is called from within the
// service function launched by Stream().
//
// If the sendTransaction func returns io.EOF, parseEvents returns ErrClientEOF.
// If the events channel is closed, parseEvents returns ErrServerEOF.
// If the context is done, returns ctx.Err().
func (bls *Streamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent) (mysql.Position, error) {
	var statements []FullBinlogStatement
	var format mysql.BinlogFormat
	var gtid mysql.GTID
	var pos = bls.startPos
	var autocommit = true
	var err error

	// Remember the RBR state.
	// tableMaps is indexed by tableID.
	tableMaps := make(map[uint64]*tableCacheEntry)

	// A begin can be triggered either by a BEGIN query, or by a GTID_EVENT.
	begin := func() {
		if statements != nil {
			// If this happened, it would be a legitimate error.
			log.Errorf("BEGIN in binlog stream while still in another transaction; dropping %d statements: %v", len(statements), statements)
			binlogStreamerErrors.Add("ParseEvents", 1)
		}
		statements = make([]FullBinlogStatement, 0, 10)
		autocommit = false
	}
	// A commit can be triggered either by a COMMIT query, or by an XID_EVENT.
	// Statements that aren't wrapped in BEGIN/COMMIT are committed immediately.
	commit := func(timestamp uint32) error {
		if int64(timestamp) >= bls.timestamp {
			eventToken := &querypb.EventToken{
				Timestamp: int64(timestamp),
				Position:  mysql.EncodePosition(pos),
			}
			if err = bls.sendTransaction(eventToken, statements); err != nil {
				if err == io.EOF {
					return ErrClientEOF
				}
				return fmt.Errorf("send reply error: %v", err)
			}
		}
		statements = nil
		autocommit = true
		return nil
	}

	// Parse events.
	for {
		var ev mysql.BinlogEvent
		var ok bool

		select {
		case ev, ok = <-events:
			if !ok {
				// events channel has been closed, which means the connection died.
				log.Infof("reached end of binlog event stream")
				return pos, ErrServerEOF
			}
		case <-ctx.Done():
			log.Infof("stopping early due to binlog Streamer service shutdown or client disconnect")
			return pos, ctx.Err()
		}

		// Validate the buffer before reading fields from it.
		if !ev.IsValid() {
			return pos, fmt.Errorf("can't parse binlog event, invalid data: %#v", ev)
		}

		// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
		// seen one, because another one might come along (e.g. on log rotate due to
		// binlog settings change) that changes the format.
		if ev.IsFormatDescription() {
			format, err = ev.Format()
			if err != nil {
				return pos, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
			}
			continue
		}

		// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
		// tells us the size of the event header.
		if format.IsZero() {
			// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
			// is a fake ROTATE_EVENT, which the master sends to tell us the name
			// of the current log file.
			if ev.IsRotate() {
				continue
			}
			return pos, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
		}

		// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
		ev, _, err = ev.StripChecksum(format)
		if err != nil {
			return pos, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
		}

		switch {
		case ev.IsGTID(): // GTID_EVENT: update current GTID, maybe BEGIN.
			var hasBegin bool
			gtid, hasBegin, err = ev.GTID(format)
			if err != nil {
				return pos, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
			}
			pos = mysql.AppendGTID(pos, gtid)
			if hasBegin {
				begin()
			}
		case ev.IsXID(): // XID_EVENT (equivalent to COMMIT)
			if err = commit(ev.Timestamp()); err != nil {
				return pos, err
			}
		case ev.IsIntVar(): // INTVAR_EVENT
			typ, value, err := ev.IntVar(format)
			if err != nil {
				return pos, fmt.Errorf("can't parse INTVAR_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, FullBinlogStatement{
				Statement: &binlogdatapb.BinlogTransaction_Statement{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
					Sql:      []byte(fmt.Sprintf("SET %s=%d", mysql.IntVarNames[typ], value)),
				},
			})
		case ev.IsRand(): // RAND_EVENT
			seed1, seed2, err := ev.Rand(format)
			if err != nil {
				return pos, fmt.Errorf("can't parse RAND_EVENT: %v, event data: %#v", err, ev)
			}
			statements = append(statements, FullBinlogStatement{
				Statement: &binlogdatapb.BinlogTransaction_Statement{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
					Sql:      []byte(fmt.Sprintf("SET @@RAND_SEED1=%d, @@RAND_SEED2=%d", seed1, seed2)),
				},
			})
		case ev.IsQuery(): // QUERY_EVENT
			// Extract the query string and group into transactions.
			q, err := ev.Query(format)
			if err != nil {
				return pos, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
			}
			switch cat := getStatementCategory(q.SQL); cat {
			case binlogdatapb.BinlogTransaction_Statement_BL_BEGIN:
				begin()
			case binlogdatapb.BinlogTransaction_Statement_BL_ROLLBACK:
				// Rollbacks are possible under some circumstances. Since the stream
				// client keeps track of its replication position by updating the set
				// of GTIDs it's seen, we must commit an empty transaction so the client
				// can update its position.
				statements = nil
				fallthrough
			case binlogdatapb.BinlogTransaction_Statement_BL_COMMIT:
				if err = commit(ev.Timestamp()); err != nil {
					return pos, err
				}
			default: // BL_DDL, BL_SET, BL_INSERT, BL_UPDATE, BL_DELETE, BL_UNRECOGNIZED
				if q.Database != "" && q.Database != bls.dbname {
					// Skip cross-db statements.
					continue
				}
				setTimestamp := &binlogdatapb.BinlogTransaction_Statement{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
					Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
				}
				statement := &binlogdatapb.BinlogTransaction_Statement{
					Category: cat,
					Sql:      []byte(q.SQL),
				}
				// If the statement has a charset and it's different than our client's
				// default charset, send it along with the statement.
				// If our client hasn't told us its charset, always send it.
				if bls.clientCharset == nil || (q.Charset != nil && !proto.Equal(q.Charset, bls.clientCharset)) {
					setTimestamp.Charset = q.Charset
					statement.Charset = q.Charset
				}
				statements = append(statements, FullBinlogStatement{
					Statement: setTimestamp,
				}, FullBinlogStatement{
					Statement: statement,
				})
				if autocommit {
					if err = commit(ev.Timestamp()); err != nil {
						return pos, err
					}
				}
			}
		case ev.IsPreviousGTIDs(): // PREVIOUS_GTIDS_EVENT
			// MySQL 5.6 only: The Binlogs contain an
			// event that gives us all the previously
			// applied commits. It is *not* an
			// authoritative value, unless we started from
			// the beginning of a binlog file.
			if !bls.usePreviousGTIDs {
				continue
			}
			newPos, err := ev.PreviousGTIDs(format)
			if err != nil {
				return pos, err
			}
			pos = newPos
			if err = commit(ev.Timestamp()); err != nil {
				return pos, err
			}
		case ev.IsTableMap():
			// Save all tables, even not in the same DB.
			tableID := ev.TableID(format)
			tm, err := ev.TableMap(format)
			if err != nil {
				return pos, err
			}
			// TODO(alainjobart) if table is already in map,
			// just use it.

			tce := &tableCacheEntry{
				tm:              tm,
				keyspaceIDIndex: -1,
			}
			tableMaps[tableID] = tce

			// Check we're in the right database, and if so, fill
			// in more data.
			if tm.Database != "" && tm.Database != bls.dbname {
				continue
			}

			// Find and fill in the table schema.
			tce.ti = bls.se.GetTable(sqlparser.NewTableIdent(tm.Name))
			if tce.ti == nil {
				return pos, fmt.Errorf("unknown table %v in schema", tm.Name)
			}

			// Fill in the resolver if needed.
			if bls.resolverFactory != nil {
				tce.keyspaceIDIndex, tce.resolver, err = bls.resolverFactory(tce.ti)
				if err != nil {
					return pos, fmt.Errorf("cannot find column to use to find keyspace_id for table %v", tm.Name)
				}
			}

			// Fill in PK indexes if necessary.
			if bls.extractPK {
				tce.pkNames = make([]*querypb.Field, len(tce.ti.PKColumns))
				tce.pkIndexes = make([]int, len(tce.ti.Columns))
				for i := range tce.pkIndexes {
					// Put -1 as default in here.
					tce.pkIndexes[i] = -1
				}
				for i, c := range tce.ti.PKColumns {
					// Patch in every PK column index.
					tce.pkIndexes[c] = i
					// Fill in pknames
					tce.pkNames[i] = &querypb.Field{
						Name: tce.ti.Columns[c].Name.String(),
						Type: tce.ti.Columns[c].Type,
					}
				}
			}
		case ev.IsWriteRows():
			tableID := ev.TableID(format)
			tce, ok := tableMaps[tableID]
			if !ok {
				return pos, fmt.Errorf("unknown tableID %v in WriteRows event", tableID)
			}
			if tce.ti == nil {
				// Skip cross-db statements.
				continue
			}
			setTimestamp := &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
			}
			statements = append(statements, FullBinlogStatement{
				Statement: setTimestamp,
			})

			rows, err := ev.Rows(format, tce.tm)
			if err != nil {
				return pos, err
			}

			statements = bls.appendInserts(statements, tce, &rows)

			if autocommit {
				if err = commit(ev.Timestamp()); err != nil {
					return pos, err
				}
			}
		case ev.IsUpdateRows():
			tableID := ev.TableID(format)
			tce, ok := tableMaps[tableID]
			if !ok {
				return pos, fmt.Errorf("unknown tableID %v in UpdateRows event", tableID)
			}
			if tce.ti == nil {
				// Skip cross-db statements.
				continue
			}
			setTimestamp := &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
			}
			statements = append(statements, FullBinlogStatement{
				Statement: setTimestamp,
			})

			rows, err := ev.Rows(format, tce.tm)
			if err != nil {
				return pos, err
			}

			statements = bls.appendUpdates(statements, tce, &rows)

			if autocommit {
				if err = commit(ev.Timestamp()); err != nil {
					return pos, err
				}
			}
		case ev.IsDeleteRows():
			tableID := ev.TableID(format)
			tce, ok := tableMaps[tableID]
			if !ok {
				return pos, fmt.Errorf("unknown tableID %v in DeleteRows event", tableID)
			}
			if tce.ti == nil {
				// Skip cross-db statements.
				continue
			}
			setTimestamp := &binlogdatapb.BinlogTransaction_Statement{
				Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
				Sql:      []byte(fmt.Sprintf("SET TIMESTAMP=%d", ev.Timestamp())),
			}
			statements = append(statements, FullBinlogStatement{
				Statement: setTimestamp,
			})

			rows, err := ev.Rows(format, tce.tm)
			if err != nil {
				return pos, err
			}

			statements = bls.appendDeletes(statements, tce, &rows)

			if autocommit {
				if err = commit(ev.Timestamp()); err != nil {
					return pos, err
				}
			}
		}
	}
}

func (bls *Streamer) appendInserts(statements []FullBinlogStatement, tce *tableCacheEntry, rows *mysql.Rows) []FullBinlogStatement {
	for i := range rows.Rows {
		var sql bytes.Buffer

		sql.WriteString("INSERT INTO ")
		sql.WriteString(tce.tm.Name)
		sql.WriteString(" SET ")

		keyspaceIDCell, pkValues, err := writeValuesAsSQL(&sql, tce, rows, i, tce.pkNames != nil)
		if err != nil {
			log.Warningf("writeValuesAsSQL(%v) failed: %v", i, err)
			continue
		}

		// Fill in keyspace id if needed.
		var ksid []byte
		if tce.resolver != nil {
			var err error
			ksid, err = tce.resolver.keyspaceID(keyspaceIDCell)
			if err != nil {
				log.Warningf("resolver(%v) failed: %v", err)
			}
		}

		statement := &binlogdatapb.BinlogTransaction_Statement{
			Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
			Sql:      sql.Bytes(),
		}
		statements = append(statements, FullBinlogStatement{
			Statement:  statement,
			Table:      tce.tm.Name,
			KeyspaceID: ksid,
			PKNames:    tce.pkNames,
			PKValues:   pkValues,
		})
	}
	return statements
}

func (bls *Streamer) appendUpdates(statements []FullBinlogStatement, tce *tableCacheEntry, rows *mysql.Rows) []FullBinlogStatement {
	for i := range rows.Rows {
		var sql bytes.Buffer

		sql.WriteString("UPDATE ")
		sql.WriteString(tce.tm.Name)
		sql.WriteString(" SET ")

		keyspaceIDCell, pkValues, err := writeValuesAsSQL(&sql, tce, rows, i, tce.pkNames != nil)
		if err != nil {
			log.Warningf("writeValuesAsSQL(%v) failed: %v", i, err)
			continue
		}

		sql.WriteString(" WHERE ")

		if _, _, err := writeIdentifiesAsSQL(&sql, tce, rows, i, false); err != nil {
			log.Warningf("writeIdentifiesAsSQL(%v) failed: %v", i, err)
			continue
		}

		// Fill in keyspace id if needed.
		var ksid []byte
		if tce.resolver != nil {
			var err error
			ksid, err = tce.resolver.keyspaceID(keyspaceIDCell)
			if err != nil {
				log.Warningf("resolver(%v) failed: %v", err)
			}
		}

		update := &binlogdatapb.BinlogTransaction_Statement{
			Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
			Sql:      sql.Bytes(),
		}
		statements = append(statements, FullBinlogStatement{
			Statement:  update,
			Table:      tce.tm.Name,
			KeyspaceID: ksid,
			PKNames:    tce.pkNames,
			PKValues:   pkValues,
		})
	}
	return statements
}

func (bls *Streamer) appendDeletes(statements []FullBinlogStatement, tce *tableCacheEntry, rows *mysql.Rows) []FullBinlogStatement {
	for i := range rows.Rows {
		var sql bytes.Buffer

		sql.WriteString("DELETE FROM ")
		sql.WriteString(tce.tm.Name)
		sql.WriteString(" WHERE ")

		keyspaceIDCell, pkValues, err := writeIdentifiesAsSQL(&sql, tce, rows, i, tce.pkNames != nil)
		if err != nil {
			log.Warningf("writeIdentifiesAsSQL(%v) failed: %v", i, err)
			continue
		}

		// Fill in keyspace id if needed.
		var ksid []byte
		if tce.resolver != nil {
			var err error
			ksid, err = tce.resolver.keyspaceID(keyspaceIDCell)
			if err != nil {
				log.Warningf("resolver(%v) failed: %v", err)
			}
		}

		statement := &binlogdatapb.BinlogTransaction_Statement{
			Category: binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
			Sql:      sql.Bytes(),
		}
		statements = append(statements, FullBinlogStatement{
			Statement:  statement,
			Table:      tce.tm.Name,
			KeyspaceID: ksid,
			PKNames:    tce.pkNames,
			PKValues:   pkValues,
		})
	}
	return statements
}

// writeValuesAsSQL is a helper method to print the values as SQL in the
// provided bytes.Buffer. It also returns the value for the keyspaceIDColumn,
// and the array of values for the PK, if necessary.
func writeValuesAsSQL(sql *bytes.Buffer, tce *tableCacheEntry, rs *mysql.Rows, rowIndex int, getPK bool) (sqltypes.Value, []sqltypes.Value, error) {
	valueIndex := 0
	data := rs.Rows[rowIndex].Data
	pos := 0
	var keyspaceIDCell sqltypes.Value
	var pkValues []sqltypes.Value
	if getPK {
		pkValues = make([]sqltypes.Value, len(tce.pkNames))
	}
	for c := 0; c < rs.DataColumns.Count(); c++ {
		if !rs.DataColumns.Bit(c) {
			continue
		}

		// Print a separator if needed, then print the name.
		if valueIndex > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(tce.ti.Columns[c].Name.String())
		sql.WriteByte('=')

		if rs.Rows[rowIndex].NullColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			sql.WriteString("NULL")
			valueIndex++
			continue
		}

		// We have real data.
		value, l, err := mysql.CellValue(data, pos, tce.tm.Types[c], tce.tm.Metadata[c], tce.ti.Columns[c].Type)
		if err != nil {
			return keyspaceIDCell, nil, err
		}
		if value.Type() == querypb.Type_TIMESTAMP && !bytes.HasPrefix(value.ToBytes(), mysql.ZeroTimestamp) {
			// Values in the binary log are UTC. Let's convert them
			// to whatever timezone the connection is using,
			// so MySQL properly converts them back to UTC.
			sql.WriteString("convert_tz(")
			value.EncodeSQL(sql)
			sql.WriteString(", '+00:00', @@session.time_zone)")
		} else {
			value.EncodeSQL(sql)
		}
		if c == tce.keyspaceIDIndex {
			keyspaceIDCell = value
		}
		if getPK {
			if tce.pkIndexes[c] != -1 {
				pkValues[tce.pkIndexes[c]] = value
			}
		}
		pos += l
		valueIndex++
	}

	return keyspaceIDCell, pkValues, nil
}

// writeIdentifiesAsSQL is a helper method to print the identifies as SQL in the
// provided bytes.Buffer. It also returns the value for the keyspaceIDColumn,
// and the array of values for the PK, if necessary.
func writeIdentifiesAsSQL(sql *bytes.Buffer, tce *tableCacheEntry, rs *mysql.Rows, rowIndex int, getPK bool) (sqltypes.Value, []sqltypes.Value, error) {
	valueIndex := 0
	data := rs.Rows[rowIndex].Identify
	pos := 0
	var keyspaceIDCell sqltypes.Value
	var pkValues []sqltypes.Value
	if getPK {
		pkValues = make([]sqltypes.Value, len(tce.pkNames))
	}
	for c := 0; c < rs.IdentifyColumns.Count(); c++ {
		if !rs.IdentifyColumns.Bit(c) {
			continue
		}

		// Print a separator if needed, then print the name.
		if valueIndex > 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(tce.ti.Columns[c].Name.String())

		if rs.Rows[rowIndex].NullIdentifyColumns.Bit(valueIndex) {
			// This column is represented, but its value is NULL.
			sql.WriteString(" IS NULL")
			valueIndex++
			continue
		}
		sql.WriteByte('=')

		// We have real data.
		value, l, err := mysql.CellValue(data, pos, tce.tm.Types[c], tce.tm.Metadata[c], tce.ti.Columns[c].Type)
		if err != nil {
			return keyspaceIDCell, nil, err
		}
		if value.Type() == querypb.Type_TIMESTAMP && !bytes.HasPrefix(value.ToBytes(), mysql.ZeroTimestamp) {
			// Values in the binary log are UTC. Let's convert them
			// to whatever timezone the connection is using,
			// so MySQL properly converts them back to UTC.
			sql.WriteString("convert_tz(")
			value.EncodeSQL(sql)
			sql.WriteString(", '+00:00', @@session.time_zone)")
		} else {
			value.EncodeSQL(sql)
		}
		if c == tce.keyspaceIDIndex {
			keyspaceIDCell = value
		}
		if getPK {
			if tce.pkIndexes[c] != -1 {
				pkValues[tce.pkIndexes[c]] = value
			}
		}
		pos += l
		valueIndex++
	}

	return keyspaceIDCell, pkValues, nil
}
