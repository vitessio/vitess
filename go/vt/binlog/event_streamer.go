// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	binlogSetInsertID     = []byte("SET INSERT_ID=")
	binlogSetInsertIDLen  = len(binlogSetInsertID)
	streamCommentStart    = []byte("/* _stream ")
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

func (evs *EventStreamer) transactionToEvent(trans *proto.BinlogTransaction) error {
	var err error
	var insertid int64
	for _, stmt := range trans.Statements {
		switch stmt.Category {
		case proto.BL_SET:
			if bytes.HasPrefix(stmt.Sql, binlogSetInsertID) {
				insertid, err = strconv.ParseInt(string(stmt.Sql[binlogSetInsertIDLen:]), 10, 64)
				if err != nil {
					binlogStreamerErrors.Add("EventStreamer", 1)
					log.Errorf("%v: %s", err, stmt.Sql)
				}
			}
		case proto.BL_DML:
			var dmlEvent *proto.StreamEvent
			dmlEvent, insertid, err = evs.buildDMLEvent(stmt.Sql, insertid)
			if err != nil {
				dmlEvent = &proto.StreamEvent{
					Category: "ERR",
					Sql:      string(stmt.Sql),
				}
			}
			dmlEvent.Timestamp = trans.Timestamp
			if err = evs.sendEvent(dmlEvent); err != nil {
				return err
			}
		case proto.BL_DDL:
			ddlEvent := &proto.StreamEvent{
				Category:  "DDL",
				Sql:       string(stmt.Sql),
				Timestamp: trans.Timestamp,
			}
			if err = evs.sendEvent(ddlEvent); err != nil {
				return err
			}
		case proto.BL_UNRECOGNIZED:
			unrecognized := &proto.StreamEvent{
				Category:  "ERR",
				Sql:       string(stmt.Sql),
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
		Category:  "POS",
		GTIDField: trans.GTIDField,
		Timestamp: trans.Timestamp,
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
// Example query: insert into vtocc_e(foo) values ('foo') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.
func (evs *EventStreamer) buildDMLEvent(sql []byte, insertid int64) (*proto.StreamEvent, int64, error) {
	// first extract the comment
	commentIndex := bytes.LastIndex(sql, streamCommentStart)
	if commentIndex == -1 {
		return nil, insertid, fmt.Errorf("missing stream comment")
	}
	dmlComment := string(sql[commentIndex+streamCommentStartLen:])

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
	dmlEvent.PKColNames, err = parsePkNames(tokenizer)
	if err != nil {
		return nil, insertid, err
	}

	// then parse the PK values, one at a time
	for typ, val = tokenizer.Scan(); typ != ';'; typ, val = tokenizer.Scan() {
		switch typ {
		case '(':
			// pkTuple is a list of pk values
			var pkTuple []interface{}
			pkTuple, insertid, err = parsePkTuple(tokenizer, insertid)
			if err != nil {
				return nil, insertid, err
			}
			if len(pkTuple) != len(dmlEvent.PKColNames) {
				return nil, insertid, fmt.Errorf("length mismatch in values")
			}
			dmlEvent.PKValues = append(dmlEvent.PKValues, pkTuple)
		default:
			return nil, insertid, fmt.Errorf("expecting '('")
		}
	}

	return dmlEvent, insertid, nil
}

// parsePkNames parses something like (eid id name )
func parsePkNames(tokenizer *sqlparser.Tokenizer) ([]string, error) {
	var columns []string
	if typ, _ := tokenizer.Scan(); typ != '(' {
		return nil, fmt.Errorf("expecting '('")
	}
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		switch typ {
		case sqlparser.ID:
			columns = append(columns, string(val))
		default:
			return nil, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
	}
	return columns, nil
}

// parsePkTuple parses something like (null 1 'bmFtZQ==' )
func parsePkTuple(tokenizer *sqlparser.Tokenizer, insertid int64) ([]interface{}, int64, error) {
	var result []interface{}

	// start scanning the list
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		switch typ {
		case '-':
			// handle negative numbers
			typ2, val2 := tokenizer.Scan()
			if typ2 != sqlparser.NUMBER {
				return nil, insertid, fmt.Errorf("expecting number after '-'")
			}
			valstr := string(val2)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				result = append(result, -ival)
			} else {
				return nil, insertid, err
			}
		case sqlparser.NUMBER:
			valstr := string(val)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				result = append(result, ival)
			} else if uval, err := strconv.ParseUint(valstr, 0, 64); err == nil {
				result = append(result, uval)
			} else {
				return nil, insertid, err
			}
		case sqlparser.NULL:
			result = append(result, insertid)
			insertid++
		case sqlparser.STRING:
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, val)
			if err != nil {
				return nil, insertid, err
			}
			result = append(result, decoded[:numDecoded])
		default:
			return nil, insertid, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
	}
	return result, insertid, nil
}
