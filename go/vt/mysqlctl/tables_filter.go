// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

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
				filtered = append(filtered, statement)
				matched = true
			case proto.BL_DML:
				// TODO(alainjobart) implement filtering
				filtered = append(filtered, statement)
				matched = true
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
