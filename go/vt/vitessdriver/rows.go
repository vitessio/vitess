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

package vitessdriver

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
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

var (
	typeInt8     = reflect.TypeOf(int8(0))
	typeUint8    = reflect.TypeOf(uint8(0))
	typeInt16    = reflect.TypeOf(int16(0))
	typeUint16   = reflect.TypeOf(uint16(0))
	typeInt32    = reflect.TypeOf(int32(0))
	typeUint32   = reflect.TypeOf(uint32(0))
	typeInt64    = reflect.TypeOf(int64(0))
	typeUint64   = reflect.TypeOf(uint64(0))
	typeFloat32  = reflect.TypeOf(float32(0))
	typeFloat64  = reflect.TypeOf(float64(0))
	typeRawBytes = reflect.TypeOf(sql.RawBytes{})
	typeTime     = reflect.TypeOf(time.Time{})
	typeUnknown  = reflect.TypeOf(new(interface{}))
)

// Implements the RowsColumnTypeScanType interface
func (ri *rows) ColumnTypeScanType(index int) reflect.Type {
	field := ri.qr.Fields[index]
	switch field.GetType() {
	case query.Type_INT8:
		return typeInt8
	case query.Type_UINT8:
		return typeUint8
	case query.Type_INT16, query.Type_YEAR:
		return typeInt16
	case query.Type_UINT16:
		return typeUint16
	case query.Type_INT24:
		return typeInt32
	case query.Type_UINT24: // no 24 bit type, using 32 instead
		return typeUint32
	case query.Type_INT32:
		return typeInt32
	case query.Type_UINT32:
		return typeUint32
	case query.Type_INT64:
		return typeInt64
	case query.Type_UINT64:
		return typeUint64
	case query.Type_FLOAT32:
		return typeFloat32
	case query.Type_FLOAT64:
		return typeFloat64
	case query.Type_TIMESTAMP, query.Type_DECIMAL, query.Type_VARCHAR, query.Type_TEXT,
		query.Type_BLOB, query.Type_VARBINARY, query.Type_CHAR, query.Type_BINARY, query.Type_BIT,
		query.Type_ENUM, query.Type_SET, query.Type_TUPLE, query.Type_GEOMETRY, query.Type_JSON,
		query.Type_HEXNUM, query.Type_HEXVAL, query.Type_BITNUM:

		return typeRawBytes
	case query.Type_DATE, query.Type_TIME, query.Type_DATETIME:
		return typeTime
	default:
		return typeUnknown
	}
}

func (ri *rows) ColumnTypeDatabaseTypeName(index int) string {
	field := ri.qr.Fields[index]
	switch field.GetType() {
	case query.Type_INT8:
		return "TINYINT"
	case query.Type_UINT8:
		return "UNSIGNED TINYINT"
	case query.Type_INT16:
		return "SMALLINT"
	case query.Type_UINT16:
		return "UNSIGNED SMALLINT"
	case query.Type_YEAR:
		return "YEAR"
	case query.Type_INT24:
		return "MEDIUMINT"
	case query.Type_UINT24:
		return "UNSIGNED MEDIUMINT"
	case query.Type_INT32:
		return "INT"
	case query.Type_UINT32:
		return "UNSIGNED INT"
	case query.Type_INT64:
		return "BIGINT"
	case query.Type_UINT64:
		return "UNSIGNED BIGINT"
	case query.Type_FLOAT32:
		return "FLOAT"
	case query.Type_FLOAT64:
		return "DOUBLE"
	case query.Type_DECIMAL:
		return "DECIMAL"
	case query.Type_VARCHAR:
		return "VARCHAR"
	case query.Type_TEXT:
		return "TEXT"
	case query.Type_BLOB:
		return "BLOB"
	case query.Type_VARBINARY:
		return "VARBINARY"
	case query.Type_CHAR:
		return "CHAR"
	case query.Type_BINARY:
		return "BINARY"
	case query.Type_BIT:
		return "BIT"
	case query.Type_ENUM:
		return "ENUM"
	case query.Type_SET:
		return "SET"
	case query.Type_HEXVAL:
		return "VARBINARY"
	case query.Type_HEXNUM:
		return "VARBINARY"
	case query.Type_BITNUM:
		return "VARBINARY"
	case query.Type_GEOMETRY:
		return "GEOMETRY"
	case query.Type_JSON:
		return "JSON"
	case query.Type_TIMESTAMP:
		return "TIMESTAMP"
	case query.Type_DATE:
		return "DATE"
	case query.Type_TIME:
		return "TIME"
	case query.Type_DATETIME:
		return "DATETIME"
	default:
		return ""
	}
}

func (ri *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	field := ri.qr.Fields[index]
	return field.GetFlags()&uint32(query.MySqlFlag_NOT_NULL_FLAG) == 0, true
}
