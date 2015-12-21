// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"github.com/youtube/vitess/go/vt/sqlparser"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var (
	binlogSetInsertID     = "SET INSERT_ID="
	binlogSetInsertIDLen  = len(binlogSetInsertID)
	streamCommentStart    = "/* _stream "
	streamCommentStartLen = len(streamCommentStart)
)

type sendEventFunc func(event *binlogdatapb.StreamEvent) error

// EventStreamer is an adapter on top of a binlog Streamer that convert
// the events into StreamEvent objects.
type EventStreamer struct {
	bls       *Streamer
	sendEvent sendEventFunc
}

// NewEventStreamer returns a new EventStreamer on top of a Streamer
func NewEventStreamer(dbname string, mysqld mysqlctl.MysqlDaemon, startPos replication.Position, sendEvent sendEventFunc) *EventStreamer {
	evs := &EventStreamer{
		sendEvent: sendEvent,
	}
	evs.bls = NewStreamer(dbname, mysqld, nil, startPos, evs.transactionToEvent)
	return evs
}

// Stream starts streaming updates
func (evs *EventStreamer) Stream(ctx *sync2.ServiceContext) error {
	return evs.bls.Stream(ctx)
}

func (evs *EventStreamer) transactionToEvent(trans *binlogdatapb.BinlogTransaction) error {
	var err error
	var insertid int64
	for _, stmt := range trans.Statements {
		switch stmt.Category {
		case binlogdatapb.BinlogTransaction_Statement_BL_SET:
			if strings.HasPrefix(stmt.Sql, binlogSetInsertID) {
				insertid, err = strconv.ParseInt(stmt.Sql[binlogSetInsertIDLen:], 10, 64)
				if err != nil {
					binlogStreamerErrors.Add("EventStreamer", 1)
					log.Errorf("%v: %s", err, stmt.Sql)
				}
			}
		case binlogdatapb.BinlogTransaction_Statement_BL_DML:
			var dmlEvent *binlogdatapb.StreamEvent
			dmlEvent, insertid, err = evs.buildDMLEvent(stmt.Sql, insertid)
			if err != nil {
				dmlEvent = &binlogdatapb.StreamEvent{
					Category: binlogdatapb.StreamEvent_SE_ERR,
					Sql:      stmt.Sql,
				}
			}
			dmlEvent.Timestamp = trans.Timestamp
			if err = evs.sendEvent(dmlEvent); err != nil {
				return err
			}
		case binlogdatapb.BinlogTransaction_Statement_BL_DDL:
			ddlEvent := &binlogdatapb.StreamEvent{
				Category:  binlogdatapb.StreamEvent_SE_DDL,
				Sql:       stmt.Sql,
				Timestamp: trans.Timestamp,
			}
			if err = evs.sendEvent(ddlEvent); err != nil {
				return err
			}
		case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
			unrecognized := &binlogdatapb.StreamEvent{
				Category:  binlogdatapb.StreamEvent_SE_ERR,
				Sql:       stmt.Sql,
				Timestamp: trans.Timestamp,
			}
			if err = evs.sendEvent(unrecognized); err != nil {
				return err
			}
		default:
			binlogStreamerErrors.Add("EventStreamer", 1)
			log.Errorf("Unrecognized event: %v: %s", stmt.Category, stmt.Sql)
		}
	}
	posEvent := &binlogdatapb.StreamEvent{
		Category:      binlogdatapb.StreamEvent_SE_POS,
		TransactionId: trans.TransactionId,
		Timestamp:     trans.Timestamp,
	}
	if err = evs.sendEvent(posEvent); err != nil {
		return err
	}
	return nil
}

/*
buildDMLEvent parses the tuples of the full stream comment.
The _stream comment is extracted into a StreamEvent.
*/
// Example query: insert into _table_(foo) values ('foo') /* _stream _table_ (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.
func (evs *EventStreamer) buildDMLEvent(sql string, insertid int64) (*binlogdatapb.StreamEvent, int64, error) {
	// first extract the comment
	commentIndex := strings.LastIndex(sql, streamCommentStart)
	if commentIndex == -1 {
		return nil, insertid, fmt.Errorf("missing stream comment")
	}
	dmlComment := sql[commentIndex+streamCommentStartLen:]

	// then strat building the response
	dmlEvent := &binlogdatapb.StreamEvent{
		Category: binlogdatapb.StreamEvent_SE_DML,
	}
	tokenizer := sqlparser.NewStringTokenizer(dmlComment)

	// first parse the table name
	typ, val := tokenizer.Scan()
	if typ != sqlparser.ID {
		return nil, insertid, fmt.Errorf("expecting table name in stream comment")
	}
	dmlEvent.TableName = string(val)

	// then parse the PK names
	var err error
	dmlEvent.PrimaryKeyFields, err = parsePkNames(tokenizer)
	hasNegatives := make([]bool, len(dmlEvent.PrimaryKeyFields))
	if err != nil {
		return nil, insertid, err
	}

	// then parse the PK values, one at a time
	for typ, val = tokenizer.Scan(); typ != ';'; typ, val = tokenizer.Scan() {
		switch typ {
		case '(':
			// pkTuple is a list of pk values
			var pkTuple *querypb.Row
			pkTuple, insertid, err = parsePkTuple(tokenizer, insertid, dmlEvent.PrimaryKeyFields, hasNegatives)
			if err != nil {
				return nil, insertid, err
			}
			dmlEvent.PrimaryKeyValues = append(dmlEvent.PrimaryKeyValues, pkTuple)
		default:
			return nil, insertid, fmt.Errorf("expecting '('")
		}
	}

	return dmlEvent, insertid, nil
}

// parsePkNames parses something like (eid id name )
func parsePkNames(tokenizer *sqlparser.Tokenizer) ([]*querypb.Field, error) {
	var columns []*querypb.Field
	if typ, _ := tokenizer.Scan(); typ != '(' {
		return nil, fmt.Errorf("expecting '('")
	}
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		switch typ {
		case sqlparser.ID:
			columns = append(columns, &querypb.Field{
				Name: string(val),
			})
		default:
			return nil, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
	}
	return columns, nil
}

// parsePkTuple parses something like (null 1 'bmFtZQ==' ). For numbers, the default
// type is Int64. If an unsigned number that can't fit in an int64 is seen, then the
// type is set to Uint64. In such cases, if a negative number was previously seen, the
// function returns an error.
func parsePkTuple(tokenizer *sqlparser.Tokenizer, insertid int64, fields []*querypb.Field, hasNegatives []bool) (*querypb.Row, int64, error) {
	result := &querypb.Row{}

	index := 0
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		if index >= len(fields) {
			return nil, insertid, fmt.Errorf("length mismatch in values")
		}

		switch typ {
		case '-':
			hasNegatives[index] = true
			typ2, val2 := tokenizer.Scan()
			if typ2 != sqlparser.NUMBER {
				return nil, insertid, fmt.Errorf("expecting number after '-'")
			}
			fullVal := append([]byte{'-'}, val2...)
			if _, err := strconv.ParseInt(string(fullVal), 0, 64); err != nil {
				return nil, insertid, err
			}
			switch fields[index].Type {
			case sqltypes.Null:
				fields[index].Type = sqltypes.Int64
			case sqltypes.Int64:
				// no-op
			default:
				return nil, insertid, fmt.Errorf("incompatible negative number field with type %v", fields[index].Type)
			}

			result.Lengths = append(result.Lengths, int64(len(fullVal)))
			result.Values = append(result.Values, fullVal...)
		case sqlparser.NUMBER:
			unsigned, err := strconv.ParseUint(string(val), 0, 64)
			if err != nil {
				return nil, insertid, err
			}
			if unsigned > uint64(9223372036854775807) {
				// Number is a uint64 that can't fit in an int64.
				if hasNegatives[index] {
					return nil, insertid, fmt.Errorf("incompatible unsigned number field with type %v", fields[index].Type)
				}
				switch fields[index].Type {
				case sqltypes.Null, sqltypes.Int64:
					fields[index].Type = sqltypes.Uint64
				case sqltypes.Uint64:
					// no-op
				default:
					return nil, insertid, fmt.Errorf("incompatible number field with type %v", fields[index].Type)
				}
			} else {
				// Could be int64 or uint64.
				switch fields[index].Type {
				case sqltypes.Null:
					fields[index].Type = sqltypes.Int64
				case sqltypes.Int64, sqltypes.Uint64:
					// no-op
				default:
					return nil, insertid, fmt.Errorf("incompatible number field with type %v", fields[index].Type)
				}
			}

			result.Lengths = append(result.Lengths, int64(len(val)))
			result.Values = append(result.Values, val...)
		case sqlparser.NULL:
			switch fields[index].Type {
			case sqltypes.Null:
				fields[index].Type = sqltypes.Int64
			case sqltypes.Int64, sqltypes.Uint64:
				// no-op
			default:
				return nil, insertid, fmt.Errorf("incompatible auto-increment field with type %v", fields[index].Type)
			}

			v := strconv.AppendInt(nil, insertid, 10)
			result.Lengths = append(result.Lengths, int64(len(v)))
			result.Values = append(result.Values, v...)
			insertid++
		case sqlparser.STRING:
			switch fields[index].Type {
			case sqltypes.Null:
				fields[index].Type = sqltypes.VarBinary
			case sqltypes.VarBinary:
				// no-op
			default:
				return nil, insertid, fmt.Errorf("incompatible string field with type %v", fields[index].Type)
			}

			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, val)
			if err != nil {
				return nil, insertid, err
			}
			result.Lengths = append(result.Lengths, int64(numDecoded))
			result.Values = append(result.Values, decoded[:numDecoded]...)
		default:
			return nil, insertid, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
		index++
	}

	if index != len(fields) {
		return nil, insertid, fmt.Errorf("length mismatch in values")
	}
	return result, insertid, nil
}
