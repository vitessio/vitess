// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysql

/*
#cgo pkg-config: gomysql
#include <stdlib.h>
#include "vtmysql.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"code.google.com/p/vitess/go/hack"
)

func init() {
	// This needs to be called before threads begin to spawn.
	C.vt_library_init()
}

type SqlError struct {
	Num     int
	Message string
	Query   string
}

func NewSqlError(number int, format string, args ...interface{}) *SqlError {
	return &SqlError{Num: number, Message: fmt.Sprintf(format, args...)}
}

func (se *SqlError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

func (se *SqlError) Number() int {
	return se.Num
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*SqlError)
		*err = terr
	}
}

type ConnectionParams struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Uname      string `json:"uname"`
	Pass       string `json:"pass"`
	Dbname     string `json:"dbname"`
	UnixSocket string `json:"unix_socket"`
	Charset    string `json:"charset"`
}

type Connection struct {
	c C.VT_CONN
}

type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]interface{}
}

type Field struct {
	Name string
	Type int64
}

func Connect(params ConnectionParams) (conn *Connection, err error) {
	defer handleError(&err)

	host := C.CString(params.Host)
	defer cfree(host)
	port := C.uint(params.Port)
	uname := C.CString(params.Uname)
	defer cfree(uname)
	pass := C.CString(params.Pass)
	defer cfree(pass)
	dbname := C.CString(params.Dbname)
	defer cfree(dbname)
	unix_socket := C.CString(params.UnixSocket)
	defer cfree(unix_socket)
	charset := C.CString(params.Charset)
	defer cfree(charset)

	conn = &Connection{}
	if C.vt_connect(&conn.c, host, uname, pass, dbname, port, unix_socket, charset, 0) != 0 {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}
	return conn, nil
}

func (conn *Connection) Close() {
	C.vt_close(&conn.c)
}

func (conn *Connection) IsClosed() bool {
	return conn.c.mysql == nil
}

func (conn *Connection) ExecuteFetch(query []byte, maxrows int, wantfields bool) (qr *QueryResult, err error) {
	if conn.IsClosed() {
		return nil, NewSqlError(2006, "Connection is closed")
	}

	if C.vt_execute(&conn.c, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query)), 0) != 0 {
		return nil, conn.lastError(query)
	}
	defer conn.CloseResult()

	qr = &QueryResult{}
	qr.RowsAffected = uint64(conn.c.affected_rows)
	qr.InsertId = uint64(conn.c.insert_id)
	if conn.c.num_fields == 0 {
		return qr, nil
	}

	if qr.RowsAffected > uint64(maxrows) {
		return nil, &SqlError{0, fmt.Sprintf("Row count exceeded %d", maxrows), string(query)}
	}
	if wantfields {
		qr.Fields = conn.Fields()
	}
	qr.Rows, err = conn.fetchAll()
	return qr, err
}

// when using ExecuteStreamFetch, use FetchNext on the Connection until it returns nil or error
func (conn *Connection) ExecuteStreamFetch(query []byte) (err error) {
	if conn.IsClosed() {
		return NewSqlError(2006, "Connection is closed")
	}
	if C.vt_execute(&conn.c, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query)), 1) != 0 {
		return conn.lastError(query)
	}
	return nil
}

func (conn *Connection) Fields() (fields []Field) {
	nfields := int(conn.c.num_fields)
	if nfields == 0 {
		return nil
	}
	cfields := (*[1 << 30]C.MYSQL_FIELD)(unsafe.Pointer(conn.c.fields))
	totalLength := uint64(0)
	for i := 0; i < nfields; i++ {
		totalLength += uint64(cfields[i].name_length)
	}
	arena := hack.NewStringArena(int(totalLength))
	fields = make([]Field, nfields)
	for i := 0; i < nfields; i++ {
		length := cfields[i].name_length
		fname := (*[1 << 30]byte)(unsafe.Pointer(cfields[i].name))[:length]
		fields[i].Name = arena.NewString(fname)
		fields[i].Type = int64(cfields[i]._type)
	}
	return fields
}

func (conn *Connection) fetchAll() (rows [][]interface{}, err error) {
	rowCount := int(conn.c.affected_rows)
	if rowCount == 0 {
		return nil, nil
	}
	rows = make([][]interface{}, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i], err = conn.FetchNext()
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (conn *Connection) FetchNext() (row []interface{}, err error) {
	vtrow := C.vt_fetch_next(&conn.c)
	if vtrow.has_error != 0 {
		return nil, conn.lastError(nil)
	}
	rowPtr := (*[1 << 30]*[1 << 30]byte)(unsafe.Pointer(vtrow.mysql_row))
	if rowPtr == nil {
		return nil, nil
	}
	colCount := int(conn.c.num_fields)
	row = make([]interface{}, colCount)
	lengths := (*[1 << 30]uint64)(unsafe.Pointer(vtrow.lengths))
	totalLength := uint64(0)
	for i := 0; i < colCount; i++ {
		totalLength += (*lengths)[i]
	}
	arena := hack.NewStringArena(int(totalLength))
	for i := 0; i < colCount; i++ {
		colLength := (*lengths)[i]
		colPtr := (*rowPtr)[i]
		if colPtr == nil {
			continue
		}
		row[i] = arena.NewString((*colPtr)[:colLength])
	}
	return row, nil
}

func (conn *Connection) CloseResult() {
	C.vt_close_result(&conn.c)
}

func (conn *Connection) Id() int64 {
	if conn.c.mysql == nil {
		return 0
	}
	return int64(C.vt_thread_id(&conn.c))
}

func (conn *Connection) lastError(query []byte) error {
	if err := C.vt_error(&conn.c); *err != 0 {
		return &SqlError{Num: int(C.vt_errno(&conn.c)), Message: C.GoString(err), Query: string(query)}
	}
	return &SqlError{0, "Dummy", string(query)}
}

func cfree(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}
