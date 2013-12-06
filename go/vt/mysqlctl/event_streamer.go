// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	BINLOG_SET_TIMESTAMP = []byte("SET TIMESTAMP=")
	BINLOG_SET_INSERT    = []byte("SET INSERT_ID=")
	STREAM_COMMENT_START = []byte("/* _stream ")
)

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
				if timestamp, err = strconv.ParseInt(string(stmt.Sql[len(BINLOG_SET_TIMESTAMP):]), 10, 64); err != nil {
					return fmt.Errorf("%v: %s", err, stmt.Sql)
				}
			} else if bytes.HasPrefix(stmt.Sql, BINLOG_SET_INSERT) {
				if insertid, err = strconv.ParseInt(string(stmt.Sql[len(BINLOG_SET_INSERT):]), 10, 64); err != nil {
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
			ddlEvent := &proto.StreamEvent{Category: "DDL", Sql: string(stmt.Sql), Timestamp: timestamp}
			if err = evs.sendEvent(ddlEvent); err != nil {
				return err
			}
		}
	}
	posEvent := &proto.StreamEvent{Category: "POS", GroupId: trans.GroupId}
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
	eventTree, err := parseStreamComment(streamComment)
	if err != nil {
		return nil, insertid, err
	}

	tableName := string(eventTree.At(0).Value)
	pkColNamesNode := eventTree.At(1)
	pkColNames := make([]string, 0, pkColNamesNode.Len())
	for _, pkCol := range pkColNamesNode.Sub {
		if pkCol.Type != sqlparser.ID {
			return nil, insertid, fmt.Errorf("expecting column name: %v", string(pkCol.Value))
		}
		pkColNames = append(pkColNames, string(pkCol.Value))
	}
	pkColLen := pkColNamesNode.Len()

	dmlEvent = new(proto.StreamEvent)
	dmlEvent.Category = "DML"
	dmlEvent.TableName = tableName
	dmlEvent.PKColNames = pkColNames
	dmlEvent.PKValues = make([][]interface{}, 0, len(eventTree.Sub[2:]))

	rowPk := make([]interface{}, pkColLen)
	for _, node := range eventTree.Sub[2:] {
		rowPk = rowPk[:0]
		if node.Len() != pkColLen {
			return nil, insertid, fmt.Errorf("length mismatch in values")
		}
		rowPk, insertid, err = encodePKValues(node.Sub, insertid)
		if err != nil {
			return nil, insertid, err
		}
		dmlEvent.PKValues = append(dmlEvent.PKValues, rowPk)
	}
	return dmlEvent, insertid, nil
	return
}

/*
parseStreamComment parses the tuples of the full stream comment.
The _stream comment is extracted into a tree node with the following structure.
EventNode.Sub[0] table name
EventNode.Sub[1] Pk column names
EventNode.Sub[2:] Pk Value lists
*/
// Example query: insert into vtocc_e(foo) values ('foo') /* _stream vtocc_e (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.
func parseStreamComment(dmlComment string) (EventNode *sqlparser.Node, err error) {
	EventNode = sqlparser.NewSimpleParseNode(sqlparser.NODE_LIST, "")
	tokenizer := sqlparser.NewStringTokenizer(dmlComment)

	node := tokenizer.Scan()
	if node.Type != sqlparser.ID {
		return nil, fmt.Errorf("expecting table name in stream comment")
	}
	EventNode.Push(node)

	for node = tokenizer.Scan(); node.Type != ';'; node = tokenizer.Scan() {
		switch node.Type {
		case '(':
			// pkTuple is a list of pk value Nodes
			pkTuple, err := parsePkTuple(tokenizer)
			if err != nil {
				return nil, err
			}
			EventNode.Push(pkTuple)
		default:
			return nil, fmt.Errorf("expecting '('")
		}
	}

	return EventNode, nil
}

// parsePkTuple parese one pk tuple.
func parsePkTuple(tokenizer *sqlparser.Tokenizer) (pkTuple *sqlparser.Node, err error) {
	// pkTuple is a list of pk value Nodes
	pkTuple = sqlparser.NewSimpleParseNode(sqlparser.NODE_LIST, "")
	// start scanning the list
	for tempNode := tokenizer.Scan(); tempNode.Type != ')'; tempNode = tokenizer.Scan() {
		switch tempNode.Type {
		case '-':
			// handle negative numbers
			t2 := tokenizer.Scan()
			if t2.Type != sqlparser.NUMBER {
				return nil, fmt.Errorf("expecing number after '-'")
			}
			t2.Value = append(tempNode.Value, t2.Value...)
			pkTuple.Push(t2)
		case sqlparser.ID, sqlparser.NUMBER, sqlparser.NULL:
			pkTuple.Push(tempNode)
		case sqlparser.STRING:
			b := tempNode.Value
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(b)))
			numDecoded, err := base64.StdEncoding.Decode(decoded, b)
			if err != nil {
				return nil, err
			}
			tempNode.Value = decoded[:numDecoded]
			pkTuple.Push(tempNode)
		default:
			return nil, fmt.Errorf("unexpected token: '%v'", string(tempNode.Value))
		}
	}
	return pkTuple, nil
}

// Interprets the parsed node and correctly encodes the primary key values.
func encodePKValues(pkValues []*sqlparser.Node, insertid int64) (rowPk []interface{}, newinsertid int64, err error) {
	for _, pkVal := range pkValues {
		if pkVal.Type == sqlparser.STRING {
			rowPk = append(rowPk, string(pkVal.Value))
		} else if pkVal.Type == sqlparser.NUMBER {
			// pkVal.Value is a byte array, convert this to string and use strconv to find
			// the right numberic type.
			valstr := string(pkVal.Value)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, ival)
			} else if uval, err := strconv.ParseUint(valstr, 0, 64); err == nil {
				rowPk = append(rowPk, uval)
			} else {
				return nil, insertid, err
			}
		} else if pkVal.Type == sqlparser.NULL {
			rowPk = append(rowPk, insertid)
			insertid++
		} else {
			return nil, insertid, fmt.Errorf("unexpected token: '%v'", string(pkVal.Value))
		}
	}
	return rowPk, insertid, nil
}
