// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/sqlannotation"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
				keyspaceID, err := sqlannotation.ExtractKeySpaceID(string(statement.Sql))
				if err != nil {
					if handleExtractKeySpaceIDError(err) {
						continue
					} else {
						// TODO(erez): Stop filtered-replication here, and alert.
						// Currently we skip.
						continue
					}
				}
				if !key.KeyRangeContains(keyrange, keyspaceID) {
					// Skip keyspace ids that don't belong to the destination shard.
					continue
				}
				filtered = append(filtered, statement)
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
