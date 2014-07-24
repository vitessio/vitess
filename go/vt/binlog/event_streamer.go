// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	BINLOG_SET_TIMESTAMP     = []byte("SET TIMESTAMP=")
	BINLOG_SET_TIMESTAMP_LEN = len(BINLOG_SET_TIMESTAMP)
	BINLOG_SET_INSERT        = []byte("SET INSERT_ID=")
	BINLOG_SET_INSERT_LEN    = len(BINLOG_SET_INSERT)
	STREAM_COMMENT_START     = []byte("/* _stream ")
)

type EventNode struct {
	Table   string
	Columns []string
	Tuples  []sqlparser.ValTuple
}

type sendEventFunc func(event *proto.StreamEvent) error

type EventStreamer struct {
	bls       *BinlogStreamer
	sendEvent sendEventFunc
}

func NewEventStreamer(dbname, binlogPrefix string) *EventStreamer {
	return &EventStreamer{
		bls: NewBinlogStreamer(dbname, binlogPrefix),
	}
}

func (evs *EventStreamer) Stream(file string, pos int64, sendEvent sendEventFunc) error {
	evs.sendEvent = sendEvent
	return evs.bls.Stream(file, pos, evs.transactionToEvent)
}

func (evs *EventStreamer) Stop() {
	evs.bls.Stop()
}

func (evs *EventStreamer) transactionToEvent(trans *proto.BinlogTransaction) error {
	var err error
	var timestamp int64
	var insertid int64
	for _, stmt := range trans.Statements {
		switch stmt.Category {
		case proto.BL_SET:
			if bytes.HasPrefix(stmt.Sql, BINLOG_SET_TIMESTAMP) {
				if timestamp, err = strconv.ParseInt(string(stmt.Sql[BINLOG_SET_TIMESTAMP_LEN:]), 10, 64); err != nil {
					return fmt.Errorf("%v: %s", err, stmt.Sql)
				}
			} else if bytes.HasPrefix(stmt.Sql, BINLOG_SET_INSERT) {
				if insertid, err = strconv.ParseInt(string(stmt.Sql[BINLOG_SET_INSERT_LEN:]), 10, 64); err != nil {
					return fmt.Errorf("%v: %s", err, stmt.Sql)
				}
			} else {
				return fmt.Errorf("unrecognized: %s", stmt.Sql)
			}
		case proto.BL_DML:
			var dmlEvent *proto.StreamEvent
			dmlEvent, insertid, err = evs.buildDMLEvent(stmt.Sql, insertid)
			if err != nil {
				return fmt.Errorf("%v: %s", err, stmt.Sql)
			}
			dmlEvent.Timestamp = timestamp
			if err = evs.sendEvent(dmlEvent); err != nil {
				return err
			}
		case proto.BL_DDL:
			ddlEvent := &proto.StreamEvent{
				Category:  "DDL",
				Sql:       string(stmt.Sql),
				Timestamp: timestamp,
			}
			if err = evs.sendEvent(ddlEvent); err != nil {
				return err
			}
		case proto.BL_UNRECOGNIZED:
			unrecognized := &proto.StreamEvent{
				Category:  "ERR",
				Sql:       string(stmt.Sql),
				Timestamp: timestamp,
			}
			if err = evs.sendEvent(unrecognized); err != nil {
				return err
			}
		}
	}
	posEvent := &proto.StreamEvent{Category: "POS", GTID: trans.GTID}
	if err = evs.sendEvent(posEvent); err != nil {
		return err
	}
	return nil
}

func (evs *EventStreamer) buildDMLEvent(sql []byte, insertid int64) (dmlEvent *proto.StreamEvent, newinsertid int64, err error) {
	commentIndex := bytes.LastIndex(sql, STREAM_COMMENT_START)
	if commentIndex == -1 {
		return &proto.StreamEvent{Category: "ERR", Sql: string(sql)}, insertid, nil
	}
	streamComment := string(sql[commentIndex+len(STREAM_COMMENT_START):])
	eventNode, err := parseStreamComment(streamComment)
	if err != nil {
		return nil, insertid, err
	}

	dmlEvent = new(proto.StreamEvent)
	dmlEvent.Category = "DML"
	dmlEvent.TableName = eventNode.Table
	dmlEvent.PKColNames = eventNode.Columns
	dmlEvent.PKValues = make([][]interface{}, 0, len(eventNode.Tuples))

	for _, tuple := range eventNode.Tuples {
		if len(tuple) != len(eventNode.Columns) {
			return nil, insertid, fmt.Errorf("length mismatch in values")
		}
		var rowPk []interface{}
		rowPk, insertid, err = encodePKValues(tuple, insertid)
		if err != nil {
			return nil, insertid, err
		}
		dmlEvent.PKValues = append(dmlEvent.PKValues, rowPk)
	}
	return dmlEvent, insertid, nil
}

/*
parseStreamComment parses the tuples of the full stream comment.
The _stream comment is extracted into an EventNode tree.
*/
// Example query: insert into vtocc_e(foo) values ('foo') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.
func parseStreamComment(dmlComment string) (eventNode EventNode, err error) {
	tokenizer := sqlparser.NewStringTokenizer(dmlComment)

	typ, val := tokenizer.Scan()
	if typ != sqlparser.ID {
		return eventNode, fmt.Errorf("expecting table name in stream comment")
	}
	eventNode.Table = string(val)

	eventNode.Columns, err = parsePkNames(tokenizer)
	if err != nil {
		return eventNode, err
	}

	for typ, val = tokenizer.Scan(); typ != ';'; typ, val = tokenizer.Scan() {
		switch typ {
		case '(':
			// pkTuple is a list of pk value Nodes
			pkTuple, err := parsePkTuple(tokenizer)
			if err != nil {
				return eventNode, err
			}
			eventNode.Tuples = append(eventNode.Tuples, pkTuple)
		default:
			return eventNode, fmt.Errorf("expecting '('")
		}
	}

	return eventNode, nil
}

func parsePkNames(tokenizer *sqlparser.Tokenizer) (columns []string, err error) {
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

// parsePkTuple parese one pk tuple.
func parsePkTuple(tokenizer *sqlparser.Tokenizer) (tuple sqlparser.ValTuple, err error) {
	// start scanning the list
	for typ, val := tokenizer.Scan(); typ != ')'; typ, val = tokenizer.Scan() {
		switch typ {
		case '-':
			// handle negative numbers
			typ2, val2 := tokenizer.Scan()
			if typ2 != sqlparser.NUMBER {
				return nil, fmt.Errorf("expecing number after '-'")
			}
			num := append(sqlparser.NumVal("-"), val2...)
			tuple = append(tuple, num)
		case sqlparser.NUMBER:
			tuple = append(tuple, sqlparser.NumVal(val))
		case sqlparser.NULL:
			tuple = append(tuple, new(sqlparser.NullVal))
		case sqlparser.STRING:
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(val)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, val)
			if err != nil {
				return nil, err
			}
			tuple = append(tuple, sqlparser.StrVal(decoded[:numDecoded]))
		default:
			return nil, fmt.Errorf("syntax error at position: %d", tokenizer.Position)
		}
	}
	return tuple, nil
}

// Interprets the parsed node and correctly encodes the primary key values.
func encodePKValues(tuple sqlparser.ValTuple, insertid int64) (rowPk []interface{}, newinsertid int64, err error) {
	for _, pkVal := range tuple {
		switch pkVal := pkVal.(type) {
		case sqlparser.StrVal:
			rowPk = append(rowPk, []byte(pkVal))
		case sqlparser.NumVal:
			valstr := string(pkVal)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, ival)
			} else if uval, err := strconv.ParseUint(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, uval)
			} else {
				return nil, insertid, err
			}
		case *sqlparser.NullVal:
			rowPk = append(rowPk, insertid)
			insertid++
		default:
			return nil, insertid, fmt.Errorf("unexpected token: '%v'", sqlparser.String(pkVal))
		}
	}
	return rowPk, insertid, nil
}
