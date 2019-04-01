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
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	binlogSetInsertID     = "SET INSERT_ID="
	binlogSetInsertIDLen  = len(binlogSetInsertID)
	streamCommentStart    = "/* _stream "
	streamCommentStartLen = len(streamCommentStart)
)

type sendEventFunc func(event *querypb.StreamEvent) error

// EventStreamer is an adapter on top of a binlog Streamer that convert
// the events into StreamEvent objects.
type EventStreamer struct {
	bls       *Streamer
	sendEvent sendEventFunc
}

// NewEventStreamer returns a new EventStreamer on top of a Streamer
func NewEventStreamer(cp *mysql.ConnParams, se *schema.Engine, startPos mysql.Position, timestamp int64, sendEvent sendEventFunc) *EventStreamer {
	evs := &EventStreamer{
		sendEvent: sendEvent,
	}
	evs.bls = NewStreamer(cp, se, nil, startPos, timestamp, evs.transactionToEvent)
	evs.bls.extractPK = true
	return evs
}

// Stream starts streaming updates
func (evs *EventStreamer) Stream(ctx context.Context) error {
	return evs.bls.Stream(ctx)
}

func (evs *EventStreamer) transactionToEvent(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
	event := &querypb.StreamEvent{
		EventToken: eventToken,
	}
	var err error
	var insertid int64
	for _, stmt := range statements {
		switch stmt.Statement.Category {
		case binlogdatapb.BinlogTransaction_Statement_BL_SET:
			sql := string(stmt.Statement.Sql)
			if strings.HasPrefix(sql, binlogSetInsertID) {
				insertid, err = strconv.ParseInt(sql[binlogSetInsertIDLen:], 10, 64)
				if err != nil {
					binlogStreamerErrors.Add("EventStreamer", 1)
					log.Errorf("%v: %s", err, sql)
				}
			}
		case binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
			binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
			binlogdatapb.BinlogTransaction_Statement_BL_DELETE:
			var dmlStatement *querypb.StreamEvent_Statement
			dmlStatement, insertid, err = evs.buildDMLStatement(stmt, insertid)
			if err != nil {
				dmlStatement = &querypb.StreamEvent_Statement{
					Category: querypb.StreamEvent_Statement_Error,
					Sql:      stmt.Statement.Sql,
				}
			}
			event.Statements = append(event.Statements, dmlStatement)
		case binlogdatapb.BinlogTransaction_Statement_BL_DDL:
			ddlStatement := &querypb.StreamEvent_Statement{
				Category: querypb.StreamEvent_Statement_DDL,
				Sql:      stmt.Statement.Sql,
			}
			event.Statements = append(event.Statements, ddlStatement)
		case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
			unrecognized := &querypb.StreamEvent_Statement{
				Category: querypb.StreamEvent_Statement_Error,
				Sql:      stmt.Statement.Sql,
			}
			event.Statements = append(event.Statements, unrecognized)
		default:
			binlogStreamerErrors.Add("EventStreamer", 1)
			log.Errorf("Unrecognized event: %v: %s", stmt.Statement.Category, stmt.Statement.Sql)
		}
	}
	return evs.sendEvent(event)
}

/*
buildDMLStatement recovers the PK from a FullBinlogStatement.
For RBR, the values are already in there, just need to be translated.
For SBR, parses the tuples of the full stream comment.
The _stream comment is extracted into a StreamEvent.Statement.
*/
// Example query: insert into _table_(foo) values ('foo') /* _stream _table_ (eid id name ) (null 1 'bmFtZQ==' ); */
// the "null" value is used for auto-increment columns.
func (evs *EventStreamer) buildDMLStatement(stmt FullBinlogStatement, insertid int64) (*querypb.StreamEvent_Statement, int64, error) {
	// For RBR events, we know all this already, just extract it.
	if stmt.PKNames != nil {
		// We get an array of []sqltypes.Value, need to convert to querypb.Row.
		dmlStatement := &querypb.StreamEvent_Statement{
			Category:         querypb.StreamEvent_Statement_DML,
			TableName:        stmt.Table,
			PrimaryKeyFields: stmt.PKNames,
			PrimaryKeyValues: []*querypb.Row{sqltypes.RowToProto3(stmt.PKValues)},
		}
		// InsertID is only needed to fill in the ID on next queries,
		// but if we use RBR, it's already in the values, so just return 0.
		return dmlStatement, 0, nil
	}

	sql := string(stmt.Statement.Sql)

	// first extract the comment
	commentIndex := strings.LastIndex(sql, streamCommentStart)
	if commentIndex == -1 {
		return nil, insertid, fmt.Errorf("missing stream comment")
	}
	dmlComment := sql[commentIndex+streamCommentStartLen:]

	// then start building the response
	dmlStatement := &querypb.StreamEvent_Statement{
		Category: querypb.StreamEvent_Statement_DML,
	}
	tokenizer := sqlparser.NewStringTokenizer(dmlComment)

	// first parse the table name
	typ, val := tokenizer.Scan()
	if typ != sqlparser.ID {
		return nil, insertid, fmt.Errorf("expecting table name in stream comment")
	}
	dmlStatement.TableName = string(val)

	// then parse the PK names
	var err error
	dmlStatement.PrimaryKeyFields, err = parsePkNames(tokenizer)
	hasNegatives := make([]bool, len(dmlStatement.PrimaryKeyFields))
	if err != nil {
		return nil, insertid, err
	}

	// then parse the PK values, one at a time
	for typ, _ = tokenizer.Scan(); typ != ';'; typ, _ = tokenizer.Scan() {
		switch typ {
		case '(':
			// pkTuple is a list of pk values
			var pkTuple *querypb.Row
			pkTuple, insertid, err = parsePkTuple(tokenizer, insertid, dmlStatement.PrimaryKeyFields, hasNegatives)
			if err != nil {
				return nil, insertid, err
			}
			dmlStatement.PrimaryKeyValues = append(dmlStatement.PrimaryKeyValues, pkTuple)
		default:
			return nil, insertid, fmt.Errorf("expecting '('")
		}
	}

	return dmlStatement, insertid, nil
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
			if typ2 != sqlparser.INTEGRAL {
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
		case sqlparser.INTEGRAL:
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
