// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/sqlannotation"

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
		filtered := make([]*binlogdatapb.BinlogTransaction_Statement, 0, len(reply.Statements))
		for _, statement := range reply.Statements {
			switch statement.Category {
			case binlogdatapb.BinlogTransaction_Statement_BL_SET:
				filtered = append(filtered, statement)
			case binlogdatapb.BinlogTransaction_Statement_BL_DDL:
				log.Warningf("Not forwarding DDL: %s", statement.Sql)
				continue
			case binlogdatapb.BinlogTransaction_Statement_BL_DML:
				keyspaceIDS, err := sqlannotation.ExtractKeyspaceIDS(string(statement.Sql))
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
					continue
				}
				query, err := getValidRangeQuery(string(statement.Sql), keyspaceIDS, keyrange)
				if err != nil {
					log.Errorf("Error parsing statement (%s). Got %v", string(statement.Sql), err)
					continue
				}
				if query == "" {
					continue
				}
				splitStatement := &binlogdatapb.BinlogTransaction_Statement{
					Category: statement.Category,
					Charset:  statement.Charset,
					Sql:      []byte(query),
				}
				filtered = append(filtered, splitStatement)
				matched = true
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

func getValidRangeQuery(sql string, keyspaceIDs [][]byte, keyrange *topodatapb.KeyRange) (query string, err error) {
	statement, err := sqlparser.Parse(sql)
	_, trailingComments := sqlparser.SplitTrailingComments(sql)
	if err != nil {
		return "", err
	}

	switch statement := statement.(type) {
	case *sqlparser.Insert:
		query, err := generateSingleInsertQuery(statement, keyspaceIDs, trailingComments, keyrange)
		if err != nil {
			return "", err
		}
		return query, nil
	default:
		return "", errors.New("unsupported construct ")
	}
}

func generateSingleInsertQuery(ins *sqlparser.Insert, keyspaceIDs [][]byte, trailingComments string, keyrange *topodatapb.KeyRange) (query string, err error) {
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return "", errors.New("unsupported: insert into select")
	case sqlparser.Values:
		var values sqlparser.Values
		if len(rows) != len(keyspaceIDs) {
			return "", fmt.Errorf("length of values tuples %v doesn't match with length of keyspaceids %v", len(values), len(keyspaceIDs))
		}
		queryBuf := sqlparser.NewTrackedBuffer(nil)
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
		queryBuf.WriteString(trailingComments)
		return queryBuf.String(), nil

	default:
		return "", errors.New("unexpected construct in insert")
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
