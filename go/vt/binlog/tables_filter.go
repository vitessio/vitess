// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"bytes"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/vt/binlog/proto"
)

var STREAM_COMMENT = []byte("/* _stream ")

// TablesFilterFunc returns a function that calls sendReply only if statements
// in the transaction match the specified tables. The resulting function can be
// passed into the BinlogStreamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, TablesFilterFunc(sendTransaction))
func TablesFilterFunc(tables []string, sendReply sendTransactionFunc) sendTransactionFunc {
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
				tableIndex := bytes.LastIndex(statement.Sql, STREAM_COMMENT)
				if tableIndex == -1 {
					updateStreamErrors.Add("TablesStream", 1)
					log.Errorf("Error parsing table name: %s", string(statement.Sql))
					continue
				}
				tableStart := tableIndex + len(STREAM_COMMENT)
				tableEnd := bytes.Index(statement.Sql[tableStart:], SPACE)
				if tableEnd == -1 {
					updateStreamErrors.Add("TablesStream", 1)
					log.Errorf("Error parsing table name: %s", string(statement.Sql))
					continue
				}
				tableName := string(statement.Sql[tableStart : tableStart+tableEnd])
				for _, t := range tables {
					if t == tableName {
						filtered = append(filtered, statement)
						matched = true
						break
					}
				}
			case proto.BL_UNRECOGNIZED:
				updateStreamErrors.Add("TablesStream", 1)
				log.Errorf("Error parsing table name: %s", string(statement.Sql))
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
