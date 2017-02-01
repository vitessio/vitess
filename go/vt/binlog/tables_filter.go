// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"strings"

	log "github.com/golang/glog"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

const (
	streamComment = "/* _stream "
	space         = " "
)

// TablesFilterFunc returns a function that calls callback only if statements
// in the transaction match the specified tables. The resulting function can be
// passed into the Streamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, TablesFilterFunc(sendTransaction))
func TablesFilterFunc(tables []string, callback sendTransactionFunc) sendTransactionFunc {
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
			case binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
				binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
				binlogdatapb.BinlogTransaction_Statement_BL_DELETE:
				sql := string(statement.Sql)
				tableIndex := strings.LastIndex(sql, streamComment)
				if tableIndex == -1 {
					updateStreamErrors.Add("TablesStream", 1)
					log.Errorf("Error parsing table name: %s", sql)
					continue
				}
				tableStart := tableIndex + len(streamComment)
				tableEnd := strings.Index(sql[tableStart:], space)
				if tableEnd == -1 {
					updateStreamErrors.Add("TablesStream", 1)
					log.Errorf("Error parsing table name: %s", sql)
					continue
				}
				tableName := sql[tableStart : tableStart+tableEnd]
				for _, t := range tables {
					if t == tableName {
						filtered = append(filtered, statement)
						matched = true
						break
					}
				}
			case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
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
		return callback(reply)
	}
}
