/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlannotation"

	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// KeyRangeFilterFunc returns a function that calls callback only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the Streamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyRangeFilterFunc(keyrange, sendTransaction))
func KeyRangeFilterFunc(keyrange *topodatapb.KeyRange, callback func(*binlogdatapb.BinlogTransaction) error) sendTransactionFunc {
	return func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		matched := false
		filtered := make([]*binlogdatapb.BinlogTransaction_Statement, 0, len(statements))
		for _, statement := range statements {
			switch statement.Statement.Category {
			case binlogdatapb.BinlogTransaction_Statement_BL_SET:
				filtered = append(filtered, statement.Statement)
			case binlogdatapb.BinlogTransaction_Statement_BL_DDL:
				log.Warningf("Not forwarding DDL: %s", statement.Statement.Sql)
				continue
			case binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
				binlogdatapb.BinlogTransaction_Statement_BL_DELETE:
				// Handle RBR case first.
				if statement.KeyspaceID != nil {
					if !key.KeyRangeContains(keyrange, statement.KeyspaceID) {
						// Skip keyspace ids that don't belong to the destination shard.
						continue
					}
					filtered = append(filtered, statement.Statement)
					matched = true
					continue
				}

				// SBR case.
				keyspaceIDS, err := sqlannotation.ExtractKeyspaceIDS(string(statement.Statement.Sql))
				if err != nil {
					if statement.Statement.Category == binlogdatapb.BinlogTransaction_Statement_BL_INSERT {
						// TODO(erez): Stop filtered-replication here, and alert.
						logExtractKeySpaceIDError(err)
						continue
					}
					// If no keyspace IDs are found, we replicate to all targets.
					// This is safe for UPDATE and DELETE because vttablet rewrites queries to
					// include the primary key and the query will only affect the shards that
					// have the rows.
					filtered = append(filtered, statement.Statement)
					matched = true
					continue
				}
				if len(keyspaceIDS) == 1 {
					if !key.KeyRangeContains(keyrange, keyspaceIDS[0]) {
						// Skip keyspace ids that don't belong to the destination shard.
						continue
					}
					filtered = append(filtered, statement.Statement)
					matched = true
					continue
				}
				query, err := getValidRangeQuery(string(statement.Statement.Sql), keyspaceIDS, keyrange)
				if err != nil {
					log.Errorf("Error parsing statement (%s). Got %v", string(statement.Statement.Sql), err)
					continue
				}
				if query == "" {
					continue
				}
				splitStatement := &binlogdatapb.BinlogTransaction_Statement{
					Category: statement.Statement.Category,
					Charset:  statement.Statement.Charset,
					Sql:      []byte(query),
				}
				filtered = append(filtered, splitStatement)
				matched = true
			case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
				updateStreamErrors.Add("KeyRangeStream", 1)
				log.Errorf("Error parsing keyspace id: %s", statement.Statement.Sql)
				continue
			}
		}
		trans := &binlogdatapb.BinlogTransaction{
			EventToken: eventToken,
		}
		if matched {
			trans.Statements = filtered
		}
		return callback(trans)
	}
}

func getValidRangeQuery(sql string, keyspaceIDs [][]byte, keyrange *topodatapb.KeyRange) (query string, err error) {
	statement, err := sqlparser.Parse(sql)
	_, marginComments := sqlparser.SplitMarginComments(sql)
	if err != nil {
		return "", err
	}

	switch statement := statement.(type) {
	case *sqlparser.Insert:
		query, err := generateSingleInsertQuery(statement, keyspaceIDs, marginComments, keyrange)
		if err != nil {
			return "", err
		}
		return query, nil
	default:
		return "", errors.New("unsupported construct ")
	}
}

func generateSingleInsertQuery(ins *sqlparser.Insert, keyspaceIDs [][]byte, marginComments sqlparser.MarginComments, keyrange *topodatapb.KeyRange) (query string, err error) {
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return "", errors.New("unsupported: insert into select")
	case sqlparser.Values:
		var values sqlparser.Values
		if len(rows) != len(keyspaceIDs) {
			return "", fmt.Errorf("length of values tuples %v doesn't match with length of keyspaceids %v", len(values), len(keyspaceIDs))
		}
		queryBuf := sqlparser.NewTrackedBuffer(nil)
		queryBuf.WriteString(marginComments.Leading)
		for rowNum, val := range rows {
			if key.KeyRangeContains(keyrange, keyspaceIDs[rowNum]) {
				values = append(values, val)
			}
		}
		if len(values) == 0 {
			return "", nil
		}
		ins.Rows = values
		ins.Format(queryBuf)
		queryBuf.WriteString(marginComments.Trailing)
		return queryBuf.String(), nil

	default:
		return "", errors.New("unexpected construct in insert")
	}
}

func logExtractKeySpaceIDError(err error) {
	extractErr, ok := err.(*sqlannotation.ExtractKeySpaceIDError)
	if !ok {
		log.Fatalf("Expected sqlannotation.ExtractKeySpaceIDError. Got: %v", err)
	}
	switch extractErr.Kind {
	case sqlannotation.ExtractKeySpaceIDParseError:
		log.Errorf(
			"Error parsing keyspace id annotation. Skipping statement. (%s)", extractErr.Message)
		updateStreamErrors.Add("ExtractKeySpaceIDParseError", 1)
	case sqlannotation.ExtractKeySpaceIDReplicationUnfriendlyError:
		log.Errorf(
			"Found replication unfriendly statement. (%s). "+
				"Filtered replication should abort, but we're currently just skipping the statement.",
			extractErr.Message)
		updateStreamErrors.Add("ExtractKeySpaceIDReplicationUnfriendlyError", 1)
	default:
		log.Fatalf("Unexpected extractErr.Kind. (%v)", extractErr)
	}
}
