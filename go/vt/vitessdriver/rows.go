/*
Copyright 2017 Google Inc.

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

package vitessdriver

import (
	"database/sql/driver"
	"io"

	"github.com/youtube/vitess/go/sqltypes"
)

// rows creates a database/sql/driver compliant Row iterator
// for a non-streaming QueryResult.
type rows struct {
	qr    *sqltypes.Result
	index int
}

// newRows creates a new rows from qr.
func newRows(qr *sqltypes.Result) driver.Rows {
	return &rows{qr: qr}
}

func (ri *rows) Columns() []string {
	cols := make([]string, 0, len(ri.qr.Fields))
	for _, field := range ri.qr.Fields {
		cols = append(cols, field.Name)
	}
	return cols
}

func (ri *rows) Close() error {
	return nil
}

func (ri *rows) Next(dest []driver.Value) error {
	if ri.index == len(ri.qr.Rows) {
		return io.EOF
	}
	populateRow(dest, ri.qr.Rows[ri.index])
	ri.index++
	return nil
}

// populateRow populates a row of data using the table's field descriptions.
// The returned types for "dest" include the list from the interface
// specification at https://golang.org/pkg/database/sql/driver/#Value
// and in addition the type "uint64" for unsigned BIGINT MySQL records.
func populateRow(dest []driver.Value, row []sqltypes.Value) {
	for i := range dest {
		dest[i], _ = sqltypes.ToNative(row[i])
	}
}
