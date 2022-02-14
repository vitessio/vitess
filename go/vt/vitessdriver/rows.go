package vitessdriver

import (
	"database/sql/driver"
	"io"

	"vitess.io/vitess/go/sqltypes"
)

// rows creates a database/sql/driver compliant Row iterator
// for a non-streaming QueryResult.
type rows struct {
	convert *converter
	qr      *sqltypes.Result
	index   int
}

// newRows creates a new rows from qr.
func newRows(qr *sqltypes.Result, c *converter) driver.Rows {
	return &rows{qr: qr, convert: c}
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
	if err := ri.convert.populateRow(dest, ri.qr.Rows[ri.index]); err != nil {
		return err
	}
	ri.index++
	return nil
}
