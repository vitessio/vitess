// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
		dest[i] = row[i].ToNative()
	}
}
