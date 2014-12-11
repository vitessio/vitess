// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bytes"
	"encoding/base64"
	"strconv"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/vt/binlog/proto"
	"github.com/henryanand/vitess/go/vt/key"
)

var KEYSPACE_ID_COMMENT = []byte("/* EMD keyspace_id:")
var SPACE = []byte(" ")

// KeyRangeFilterFunc returns a function that calls sendReply only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the BinlogStreamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyRangeFilterFunc(sendTransaction))
func KeyRangeFilterFunc(kit key.KeyspaceIdType, keyrange key.KeyRange, sendReply sendTransactionFunc) sendTransactionFunc {
	isInteger := true
	if kit == key.KIT_BYTES {
		isInteger = false
	}

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
				keyspaceIndex := bytes.LastIndex(statement.Sql, KEYSPACE_ID_COMMENT)
				if keyspaceIndex == -1 {
					updateStreamErrors.Add("KeyRangeStream", 1)
					log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
					continue
				}
				idstart := keyspaceIndex + len(KEYSPACE_ID_COMMENT)
				idend := bytes.Index(statement.Sql[idstart:], SPACE)
				if idend == -1 {
					updateStreamErrors.Add("KeyRangeStream", 1)
					log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
					continue
				}
				textId := string(statement.Sql[idstart : idstart+idend])
				if isInteger {
					id, err := strconv.ParseUint(textId, 10, 64)
					if err != nil {
						updateStreamErrors.Add("KeyRangeStream", 1)
						log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
						continue
					}
					if !keyrange.Contains(key.Uint64Key(id).KeyspaceId()) {
						continue
					}
				} else {
					data, err := base64.StdEncoding.DecodeString(textId)
					if err != nil {
						updateStreamErrors.Add("KeyRangeStream", 1)
						log.Errorf("Error parsing keyspace id: %s", string(statement.Sql))
						continue
					}
					if !keyrange.Contains(key.KeyspaceId(data)) {
						continue
					}
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
