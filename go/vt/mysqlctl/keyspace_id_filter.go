// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/key"
)

var KEYSPACE_ID_COMMENT = []byte("/* EMD keyspace_id:")
var SPACE = []byte(" ")

// KeyspaceIdFilterFunc returns a function that calls sendReply only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the BinlogStreamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyspaceIdFilterFunc(sendTransaction))
func KeyspaceIdFilterFunc(keyrange key.KeyRange, sendReply sendTransactionFunc) sendTransactionFunc {
	return func(reply *BinlogTransaction) error {
		matched := false
		filtered := make([]Statement, 0, len(reply.Statements))
		for _, statement := range reply.Statements {
			switch statement.Category {
			case BL_SET:
				filtered = append(filtered, statement)
			case BL_DDL:
				filtered = append(filtered, statement)
				matched = true
			case BL_DML:
				keyspaceIndex := bytes.LastIndex(statement.Sql, KEYSPACE_ID_COMMENT)
				if keyspaceIndex == -1 {
					// TODO(sougou): increment error counter
					log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
					continue
				}
				idstart := keyspaceIndex + len(KEYSPACE_ID_COMMENT)
				idend := bytes.Index(statement.Sql[idstart:], SPACE)
				if idend == -1 {
					// TODO(sougou): increment error counter
					log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
					continue
				}
				id, err := strconv.ParseUint(string(statement.Sql[idstart:idstart+idend]), 10, 64)
				if err != nil {
					// TODO(sougou): increment error counter
					log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
					continue
				}
				if !keyrange.Contains(key.Uint64Key(id).KeyspaceId()) {
					continue
				}
				filtered = append(filtered, statement)
				matched = true
			}
		}
		if !matched {
			return nil
		}
		reply.Statements = filtered
		return sendReply(reply)
	}
}
