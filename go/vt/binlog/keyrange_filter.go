// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	annotations "github.com/youtube/vitess/go/vt/sqlannotations"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var KEYSPACE_ID_COMMENT = []byte("/* vtgate:: keyspace_id:")
var SPACE = []byte(" ")

// KeyRangeFilterFunc returns a function that calls sendReply only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the BinlogStreamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyRangeFilterFunc(sendTransaction))
func KeyRangeFilterFunc(kit key.KeyspaceIdType, keyrange *pb.KeyRange, sendReply sendTransactionFunc) sendTransactionFunc {
	return func(reply *proto.BinlogTransaction) error {
		matched := false
		filtered := make([]proto.Statement, 0, len(reply.Statements))
		for _, statement := range reply.Statements {
			switch statement.Category {
			case proto.BL_SET:
				filtered = append(filtered, statement)
			case proto.BL_DDL:
				log.Warningf("Not forwarding DDL: %s", string(statement.Sql))
				continue
			case proto.BL_DML:
				keyspaceId, unfriendly, err := annotations.ParseSQLAnnotation(string(statement.Sql))
				if err != nil {
					updateStreamErrors.Add("KeyRangeStream", 1)
					log.Errorf(
						"Error parsing keyspace id annotation. Skipping statement: %s, (%s)",
						string(statement.Sql), err)
					continue
				}
				if unfriendly {
					updateStreamErrors.Add("KeyRangeStream", 1)
					log.Errorf(
						"Skipping filtered-replication-unfriendly DML statement: %s",
						string(statement.Sql))
					continue
				}
				if !key.KeyRangeContains(keyrange, keyspaceId) {
					continue
				}
				filtered = append(filtered, statement)
				matched = true
			case proto.BL_UNRECOGNIZED:
				updateStreamErrors.Add("KeyRangeStream", 1)
				log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
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
