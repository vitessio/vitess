// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/sqlannotation"

	"encoding/hex"
	"errors"
	"fmt"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// KeyRangeFilterFunc returns a function that calls sendReply only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the Streamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyRangeFilterFunc(keyrange, sendTransaction))
func KeyRangeFilterFunc(keyrange *topodatapb.KeyRange, sendReply sendTransactionFunc) sendTransactionFunc {
	return func(reply *binlogdatapb.BinlogTransaction) error {
		matched := false
		filtered := []*binlogdatapb.BinlogTransaction_Statement{}
		for _, statement := range reply.Statements {
			switch statement.Category {
			case binlogdatapb.BinlogTransaction_Statement_BL_SET:
				filtered = append(filtered, statement)
			case binlogdatapb.BinlogTransaction_Statement_BL_DDL:
				log.Warningf("Not forwarding DDL: %s", statement.Sql)
				continue
			case binlogdatapb.BinlogTransaction_Statement_BL_DML:
				keyspaceIDS, err := sqlannotation.ExtractKeySpaceIDS(string(statement.Sql))
				if err != nil {
					if handleExtractKeySpaceIDError(err) {
						continue
					} else {
						// TODO(erez): Stop filtered-replication here, and alert.
						// Currently we skip.
						continue
					}
				}
				if len(keyspaceIDS) == 1 {
					if !key.KeyRangeContains(keyrange, keyspaceIDS[0]) {
						// Skip keyspace ids that don't belong to the destination shard.
						continue
					}
					filtered = append(filtered, statement)
					matched = true
				} else {
					multipleQueries, err := splitMultiValueInsertQuery(string(statement.Sql), keyspaceIDS)
					if err != nil {
						continue
					}
					for rowNum, query := range multipleQueries {
						if !key.KeyRangeContains(keyrange, keyspaceIDS[rowNum]) {
							// Skip keyspace ids that don't belong to the destination shard.
							continue
						}
						splitStatement := &binlogdatapb.BinlogTransaction_Statement{
							Category: statement.Category,
							Charset:  statement.Charset,
							Sql:      []byte(query),
						}
						filtered = append(filtered, splitStatement)
					}
					matched = true
				}
			case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
				updateStreamErrors.Add("KeyRangeStream", 1)
				log.Errorf("Error parsing keyspace id: %s", statement.Sql)
				continue
			}
		}
		if matched {
			reply.Statements = filtered
		} else {
			reply.Statements = nil
		}
		return sendReply(reply)
	}
}

func splitMultiValueInsertQuery(sql string, keyspaceIDs [][]byte) (queries []string, err error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	switch statement := statement.(type) {
	case *sqlparser.Insert:
		trailingComments := sqlannotation.ExtractTrailingComments(sql, "/* vtgate:: keyspace_id:", "*/")
		queries, err := generateSingleInsertQueries(statement, keyspaceIDs, trailingComments)
		if err != nil {
			return nil, err
		}
		return queries, nil
	default:
		return nil, errors.New("unsupported construct ")
	}
}

func generateSingleInsertQueries(ins *sqlparser.Insert, keyspaceIDs [][]byte, trailingComments string) (queries []string, err error) {
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return nil, errors.New("unsupported: insert into select")
	case sqlparser.Values:
		values = rows
		if len(values) != len(keyspaceIDs) {
			return nil, fmt.Errorf("length of values tuples %v doesn't match with length of keyspaceids %v", len(values), len(keyspaceIDs))
		}
		queryBuf := sqlparser.NewTrackedBuffer(nil)
		queries := make([]string, len(values))
		for rowNum, val := range values {
			queryBuf.Myprintf("insert %v%sinto %v%v values%v%v /* vtgate:: keyspace_id:%s */%s",
				ins.Comments, ins.Ignore,
				ins.Table, ins.Columns, val, ins.OnDup, hex.EncodeToString(keyspaceIDs[rowNum]), trailingComments)
			queries[rowNum] = queryBuf.String()
			queryBuf.Truncate(0)
		}
		return queries, nil

	default:
		return nil, errors.New("unexpected construct in insert")
	}
}

// Handles the error in sqlannotation.ExtractKeySpaceIDError.
// Returns 'true' iff filtered replication should continue (and skip the current SQL
// statement).
// TODO(erez): Currently, always returns true. So filtered-replication-unfriendly
// statemetns also get skipped. We need to abort filtered-replication in a
// graceful manner.
func handleExtractKeySpaceIDError(err error) bool {
	extractErr, ok := err.(*sqlannotation.ExtractKeySpaceIDError)
	if !ok {
		log.Fatalf("Expected sqlannotation.ExtractKeySpaceIDError. Got: %v", err)
	}
	switch extractErr.Kind {
	case sqlannotation.ExtractKeySpaceIDParseError:
		log.Errorf(
			"Error parsing keyspace id annotation. Skipping statement. (%s)", extractErr.Message)
		updateStreamErrors.Add("ExtractKeySpaceIDParseError", 1)
		return true
	case sqlannotation.ExtractKeySpaceIDReplicationUnfriendlyError:
		log.Errorf(
			"Found replication unfriendly statement. (%s). "+
				"Filtered replication should abort, but we're currenty just skipping the statement.",
			extractErr.Message)
		updateStreamErrors.Add("ExtractKeySpaceIDReplicationUnfriendlyError", 1)
		return true
	default:
		log.Fatalf("Unexpected extractErr.Kind. (%v)", extractErr)
		return true // Unreachable.
	}

}
