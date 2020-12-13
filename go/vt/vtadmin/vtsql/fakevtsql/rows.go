package fakevtsql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrBadRow is returned from Next() when a row has an incorrect number of
	// fields.
	ErrBadRow = errors.New("bad sql row")
	// ErrRowsClosed is returned when attempting to operate on an already-closed
	// Rows.
	ErrRowsClosed = errors.New("err rows closed")
)

type rows struct {
	cols []string
	vals [][]interface{}
	pos  int

	closed bool
}

var _ driver.Rows = (*rows)(nil)

func (r *rows) Close() error {
	r.closed = true
	return nil
}

func (r *rows) Columns() []string {
	return r.cols
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed {
		return ErrRowsClosed
	}

	if r.pos >= len(r.vals) {
		return io.EOF
	}

	row := r.vals[r.pos]
	r.pos++

	if len(row) != len(r.cols) {
		return fmt.Errorf("%w: row %d has %d fields but %d cols", ErrBadRow, r.pos-1, len(row), len(r.cols))
	}

	for i := 0; i < len(r.cols); i++ {
		dest[i] = row[i]
	}

	return nil
}
