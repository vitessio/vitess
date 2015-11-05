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
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"

	pb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

var (
	binlogSetInsertID     = "SET INSERT_ID="
	binlogSetInsertIDLen  = len(binlogSetInsertID)
	streamCommentStart    = "/* _stream "
	streamCommentStartLen = len(streamCommentStart)
)

type sendEventFunc func(event *proto.StreamEvent) error

// EventStreamer is an adapter on top of a BinlogStreamer that convert
// the events into StreamEvent objects.
type EventStreamer struct {
	bls       *BinlogStreamer
	sendEvent sendEventFunc
}

// NewEventStreamer returns a new EventStreamer on top of a BinlogStreamer
func NewEventStreamer(dbname string, mysqld mysqlctl.MysqlDaemon, startPos myproto.ReplicationPosition, sendEvent sendEventFunc) *EventStreamer {
	evs := &EventStreamer{
		sendEvent: sendEvent,
	}
	evs.bls = NewBinlogStreamer(dbname, mysqld, nil, startPos, evs.transactionToEvent)
	return evs
}

// Stream starts streaming updates
func (evs *EventStreamer) Stream(ctx *sync2.ServiceContext) error {
	return evs.bls.Stream(ctx)
}

func (evs *EventStreamer) transactionToEvent(trans *pb.BinlogTransaction) error {
	var err error
	var insertid int64
	for _, stmt := range trans.Statements {
		switch stmt.Category {
		case pb.BinlogTransaction_Statement_BL_SET:
			if strings.HasPrefix(stmt.Sql, binlogSetInsertID) {
				insertid, err = strconv.ParseInt(stmt.Sql[binlogSetInsertIDLen:], 10, 64)
				if err != nil {
					binlogStreamerErrors.Add("EventStreamer", 1)
					log.Errorf("%v: %s", err, stmt.Sql)
				}
			}
		case pb.BinlogTransaction_Statement_BL_DML:
			var dmlEvent *proto.StreamEvent
			dmlEvent, insertid, err = evs.buildDMLEvent(stmt.Sql, insertid)
			if err != nil {
				dmlEvent = &proto.StreamEvent{
					Category: "ERR",
					Sql:      stmt.Sql,
				}
			}
			dmlEvent.Timestamp = trans.Timestamp
			if err = evs.sendEvent(dmlEvent); err != nil {
				return err
			}
		case pb.BinlogTransaction_Statement_BL_DDL:
			ddlEvent := &proto.StreamEvent{
				Category:  "DDL",
				Sql:       stmt.Sql,
				Timestamp: trans.Timestamp,
			}
			if err = evs.sendEvent(ddlEvent); err != nil {
				return err
			}
		case pb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
			unrecognized := &proto.StreamEvent{
				Category:  "ERR",
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
	posEvent := &proto.StreamEvent{
		Category:      "POS",
		TransactionID: trans.TransactionId,
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
func (evs *EventStreamer) buildDMLEvent(sql string, insertid int64) (*proto.StreamEvent, int64, error) {
	// first extract the comment
	commentIndex := strings.LastIndex(sql, streamCommentStart)
	if commentIndex == -1 {
		return nil, insertid, fmt.Errorf("missing stream comment")
	}
	dmlComment := sql[commentIndex+streamCommentStartLen:]

	// then strat building the response
	dmlEvent := &proto.StreamEvent{
		Category: "DML",
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
	if err != nil {
		return nil, insertid, err
	}

	// then parse the PK values, one at a time
	for typ, val = tokenizer.Scan(); typ != ';'; typ, val = tokenizer.Scan() {
		switch typ {
		case '(':
			// pkTuple is a list of pk values
			var pkTuple []sqltypes.Value
			pkTuple, insertid, err = parsePkTuple(tokenizer, insertid, dmlEvent.PrimaryKeyFields)
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
func parsePkNames(tokenizer *sqlparser.Tokenizer) ([]mproto.Field, error) {
	var columns []mproto.Field
	if typ, _ := tokenizer.Scan(); typ != '(' {
		return nil, fmt.Errorf("expecting '('")
	}
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		switch typ {
		case sqlparser.ID:
			columns = append(columns, mproto.Field{
				Name: string(val),
			})
		default:
			return nil, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
	}
	return columns, nil
}

// parsePkTuple parses something like (null 1 'bmFtZQ==' )
func parsePkTuple(tokenizer *sqlparser.Tokenizer, insertid int64, fields []mproto.Field) ([]sqltypes.Value, int64, error) {
	var result []sqltypes.Value

	// start scanning the list
	index := 0
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		if index >= len(fields) {
			return nil, insertid, fmt.Errorf("length mismatch in values")
		}

		switch typ {
		case '-':
			// handle negative numbers
			typ2, val2 := tokenizer.Scan()
			if typ2 != sqlparser.NUMBER {
				return nil, insertid, fmt.Errorf("expecting number after '-'")
			}

			// check value
			fullVal := append([]byte{'-'}, val2...)
			if _, err := strconv.ParseInt(string(fullVal), 0, 64); err != nil {
				return nil, insertid, err
			}

			// update type
			switch fields[index].Type {
			case mproto.VT_DECIMAL:
				// we haven't updated the type yet
				fields[index].Type = mproto.VT_LONGLONG
			case mproto.VT_LONGLONG:
				// nothing to do there
			default:
				// we already set this to something incompatible!
				return nil, insertid, fmt.Errorf("incompatible negative number field with type %v", fields[index].Type)
			}

			// update value
			result = append(result, sqltypes.MakeNumeric(fullVal))

		case sqlparser.NUMBER:
			// check value
			if _, err := strconv.ParseUint(string(val), 0, 64); err != nil {
				return nil, insertid, err
			}

			// update type
			switch fields[index].Type {
			case mproto.VT_DECIMAL:
				// we haven't updated the type yet
				fields[index].Type = mproto.VT_LONGLONG
			case mproto.VT_LONGLONG:
				// nothing to do there
			default:
				// we already set this to something incompatible!
				return nil, insertid, fmt.Errorf("incompatible number field with type %v", fields[index].Type)
			}

			// update value
			result = append(result, sqltypes.MakeNumeric(val))
		case sqlparser.NULL:
			// update type
			switch fields[index].Type {
			case mproto.VT_DECIMAL:
				// we haven't updated the type yet
				fields[index].Type = mproto.VT_LONGLONG
			case mproto.VT_LONGLONG:
				// nothing to do there
			default:
				// we already set this to something incompatible!
				return nil, insertid, fmt.Errorf("incompatible auto-increment field with type %v", fields[index].Type)
			}

			// update value
			result = append(result, sqltypes.MakeNumeric(strconv.AppendInt(nil, insertid, 10)))
			insertid++
		case sqlparser.STRING:
			// update type
			switch fields[index].Type {
			case mproto.VT_DECIMAL:
				// we haven't updated the type yet
				fields[index].Type = mproto.VT_VAR_STRING
			case mproto.VT_VAR_STRING:
				// nothing to do there
			default:
				// we already set this to something incompatible!
				return nil, insertid, fmt.Errorf("incompatible string field with type %v", fields[index].Type)
			}

			// update value
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, val)
			if err != nil {
				return nil, insertid, err
			}
			result = append(result, sqltypes.MakeString(decoded[:numDecoded]))
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
