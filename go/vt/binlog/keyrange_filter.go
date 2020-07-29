/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"fmt"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// KeyRangeFilterFunc returns a function that calls callback only if statements
// in the transaction match the specified keyrange. The resulting function can be
// passed into the Streamer: bls.Stream(file, pos, sendTransaction) ->
// bls.Stream(file, pos, KeyRangeFilterFunc(keyrange, sendTransaction))
func KeyRangeFilterFunc(keyrange *topodatapb.KeyRange, callback func(*binlogdatapb.BinlogTransaction) error) sendTransactionFunc {
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
				if statement.KeyspaceID == nil {
					updateStreamErrors.Add("KeyRangeStream", 1)
					return fmt.Errorf("SBR mode unsupported for streaming: %s", statement.Statement.Sql)
				}
				if !key.KeyRangeContains(keyrange, statement.KeyspaceID) {
					continue
				}
				filtered = append(filtered, statement.Statement)
				matched = true
			case binlogdatapb.BinlogTransaction_Statement_BL_UNRECOGNIZED:
				updateStreamErrors.Add("KeyRangeStream", 1)
				log.Errorf("Error parsing keyspace id: %s", statement.Statement.Sql)
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
