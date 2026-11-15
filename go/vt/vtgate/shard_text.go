/*
Copyright 2026 The Vitess Authors.

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

package vtgate

import (
	"sync"

	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// With --convert-result-charset the text of a row is in the character set of
// the collation in its field, and the rows of several shards become one
// result with the fields of the shard that answered first. The tablets of two
// shards do not have to send the same character set: one with
// --queryserver-config-raw-result-charset sends a latin1_swedish_ci column
// with 'Müller' as 4D FC 6C 6C 65 72 with the collation 8, and one without it
// sends 4D C3 BC 6C 6C 65 72 with the collation 255. The rows of every shard
// are converted to the character sets of the first fields, so that the result
// is in the character set its fields say.

// shardResult returns the result of a shard with its text in the character
// sets of fields, which are the fields of the result that it is appended to.
// It never changes the result of the shard.
func shardResult(fields []*querypb.Field, qr *sqltypes.Result) *sqltypes.Result {
	if len(fields) == 0 || len(qr.Fields) == 0 || len(qr.Rows) == 0 {
		return qr
	}
	conv := resultcharset.ToFields(qr.Fields, fields)
	if !conv.ConvertsValues() {
		return qr
	}
	return withRows(qr, fields, conv.Rows(qr.Rows))
}

// withRows returns a copy of qr with other fields and rows.
func withRows(qr *sqltypes.Result, fields []*querypb.Field, rows []sqltypes.Row) *sqltypes.Result {
	return &sqltypes.Result{
		Fields:              fields,
		RowsAffected:        qr.RowsAffected,
		InsertID:            qr.InsertID,
		InsertIDChanged:     qr.InsertIDChanged,
		Rows:                rows,
		SessionStateChanges: qr.SessionStateChanges,
		StatusFlags:         qr.StatusFlags,
		Info:                qr.Info,
	}
}

// shardStreams converts the streams of the shards of one statement to the
// character sets of the fields of the first shard that sent its fields.
type shardStreams struct {
	mu     sync.Mutex
	fields []*querypb.Field
}

// stream returns the callback for the stream of one shard. The fields of a
// stream arrive before its rows. When the shard does not send the character
// sets of the first fields, its rows are converted and its fields are replaced
// by the first ones, so that whichever fields the reader of the streams takes,
// they describe every row.
func (s *shardStreams) stream(callback func(*sqltypes.Result) error) func(*sqltypes.Result) error {
	var conv *resultcharset.Conversion
	var fields []*querypb.Field
	return func(qr *sqltypes.Result) error {
		if qr == nil {
			return callback(qr)
		}
		if len(qr.Fields) != 0 {
			s.mu.Lock()
			if s.fields == nil {
				s.fields = qr.Fields
			}
			fields = s.fields
			s.mu.Unlock()
			if conv = resultcharset.ToFields(qr.Fields, fields); !conv.ConvertsValues() {
				conv = nil
			}
		}
		if conv == nil {
			return callback(qr)
		}
		var first []*querypb.Field
		if len(qr.Fields) != 0 {
			first = fields
		}
		return callback(withRows(qr, first, conv.Rows(qr.Rows)))
	}
}
