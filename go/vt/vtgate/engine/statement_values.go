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

package engine

import (
	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// statementValues converts the values of the rows of one result for a
// statement that vtgate sends itself, or for a vindex.
//
// With --convert-result-charset the text of a value of a row is in the
// character set of the collation in its field: a latin1_swedish_ci column with
// 'Müller' is 4D FC 6C 6C 65 72. Every statement, and so every bind variable,
// is in the connection character set, utf8mb4, and a vindex hashes the bytes
// of a value. So a value of a row that becomes a bind variable, for the other
// side of a join, the outer query of a subquery, the rows of an INSERT ...
// SELECT or the entry of a lookup vindex, has to become 4D C3 BC 6C 6C 65 72
// first. MySQL converts it back when it compares it with the column.
//
// The value has to be converted where it leaves its row, because a bind
// variable does not say which character set it is in. Without the flag every
// value stays as it is: a tablet then sends its rows in the character set of
// its connection, which is the one its statements are in.
type statementValues struct {
	conv *resultcharset.Conversion
}

// newStatementValues returns the conversion of the values of the rows of a
// result with these fields. Without fields the values stay as they are, so
// the result that feeds a statement has to be asked for with its fields.
func newStatementValues(vcursor VCursor, fields []*querypb.Field) statementValues {
	if !vcursor.ConvertResultCharset() || len(fields) == 0 {
		return statementValues{}
	}
	conv := resultcharset.ToConnection(fields, vcursor.ConnCollation(), true)
	if !conv.ConvertsValues() {
		return statementValues{}
	}
	return statementValues{conv: conv}
}

// value returns the value of the column col of a row in the connection
// character set.
func (s statementValues) value(col int, value sqltypes.Value) sqltypes.Value {
	if s.conv == nil {
		return value
	}
	return s.conv.Value(col, value)
}

// row returns the values of a row in the connection character set. A row
// without a value that changes is the row it was.
func (s statementValues) row(row sqltypes.Row) sqltypes.Row {
	if s.conv == nil {
		return row
	}
	return s.conv.Rows([]sqltypes.Row{row})[0]
}

// rows returns the rows in the connection character set. A row without a
// value that changes is the row it was.
func (s statementValues) rows(rows []sqltypes.Row) []sqltypes.Row {
	if s.conv == nil {
		return rows
	}
	return s.conv.Rows(rows)
}

// streamStatementValues is statementValues for the chunks of one stream: its
// fields come with the first chunk, and convert the rows of every chunk. It is
// not safe for concurrent use.
type streamStatementValues struct {
	values     statementValues
	haveFields bool
}

// of returns the conversion of the values of the rows of a chunk of the
// stream, by the fields of the first chunk that has fields.
func (s *streamStatementValues) of(vcursor VCursor, chunk *sqltypes.Result) statementValues {
	if !s.haveFields && len(chunk.Fields) != 0 {
		s.haveFields = true
		s.values = newStatementValues(vcursor, chunk.Fields)
	}
	return s.values
}
