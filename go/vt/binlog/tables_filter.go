// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"strings"

	log "github.com/golang/glog"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

const (
	streamComment = "/* _stream "
	space         = " "
)

// TablesFilterFunc returns a function that calls callback only if statements
// in the transaction match the specified tables. The resulting function can be
// passed into the Streamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, TablesFilterFunc(sendTransaction))
func TablesFilterFunc(tables []string, callback func(*binlogdatapb.BinlogTransaction) error) sendTransactionFunc {
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
				tableName := statement.Table
				if tableName == "" {
					// The statement doesn't
					// contain the table name (SBR
					// event), figure it out.
					sql := string(statement.Statement.Sql)
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
					tableName = sql[tableStart : tableStart+tableEnd]
				}
				for _, t := range tables {
					if t == tableName {
						filtered = append(filtered, statement.Statement)
						matched = true
						break
					}
				}
			case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
				updateStreamErrors.Add("TablesStream", 1)
				log.Errorf("Error parsing table name: %s", string(statement.Statement.Sql))
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
