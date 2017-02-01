// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mysql wraps the C client library for MySQL.
package mysql

/*
#cgo CFLAGS: -Werror=implicit
#cgo pkg-config: gomysql
#include <stdlib.h>
#include <mysqld_error.h>
#include "vtmysql.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

const (
	maxSize = 1 << 32
)

func init() {
	// This needs to be called before threads begin to spawn.
	C.vt_library_init()
	sqldb.Register("libmysqlclient", Connect)

	// Comment this out and uncomment call to sqldb.RegisterDefault in
	// go/mysqlconn/sqldb_conn.go to make it the default.
	sqldb.RegisterDefault(Connect)
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*sqldb.SQLError)
		*err = terr
	}
}

// Connection encapsulates a C mysql library connection
type Connection struct {
	c C.VT_CONN
}

// Connect uses the connection parameters to connect and returns the connection
func Connect(params sqldb.ConnParams) (sqldb.Conn, error) {
	var err error
	defer handleError(&err)

	host := C.CString(params.Host)
	defer cfree(host)
	port := C.uint(params.Port)
	uname := C.CString(params.Uname)
	defer cfree(uname)
	pass := C.CString(params.Pass)
	defer cfree(pass)
	dbname := C.CString(params.DbName)
	defer cfree(dbname)
	unixSocket := C.CString(params.UnixSocket)
	defer cfree(unixSocket)
	charset := C.CString(params.Charset)
	defer cfree(charset)
	flags := C.ulong(params.Flags)

	conn := &Connection{}
	if C.vt_connect(&conn.c, host, uname, pass, dbname, port, unixSocket, charset, flags) != 0 {
		defer conn.Close()
		return nil, conn.lastError("")
	}
	return conn, nil
}

// Close closes the mysql connection
func (conn *Connection) Close() {
	C.vt_close(&conn.c)
}

// IsClosed returns if the connection was ever closed
func (conn *Connection) IsClosed() bool {
	return conn.c.mysql == nil
}

// ExecuteFetch executes the query on the connection
func (conn *Connection) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *sqltypes.Result, err error) {
	if conn.IsClosed() {
		return nil, sqldb.NewSQLError(2006, "", "Connection is closed")
	}

	if C.vt_execute(&conn.c, (*C.char)(hack.StringPointer(query)), C.ulong(len(query)), 0) != 0 {
		return nil, conn.lastError(query)
	}
	defer conn.CloseResult()

	qr = &sqltypes.Result{}
	qr.RowsAffected = uint64(conn.c.affected_rows)
	qr.InsertID = uint64(conn.c.insert_id)
	if conn.c.num_fields == 0 {
		return qr, nil
	}

	if qr.RowsAffected > uint64(maxrows) {
		return nil, &sqldb.SQLError{
			Num:     0,
			Message: fmt.Sprintf("Row count exceeded %d", maxrows),
			Query:   query,
		}
	}
	if wantfields {
		qr.Fields, err = conn.Fields()
		if err != nil {
			return nil, err
		}
	}
	qr.Rows, err = conn.fetchAll()
	return qr, err
}

// ExecuteStreamFetch starts a streaming query to mysql. Use FetchNext
// on the Connection until it returns nil or error
func (conn *Connection) ExecuteStreamFetch(query string) (err error) {
	if conn.IsClosed() {
		return sqldb.NewSQLError(2006, "", "Connection is closed")
	}
	if C.vt_execute(&conn.c, (*C.char)(hack.StringPointer(query)), C.ulong(len(query)), 1) != 0 {
		return conn.lastError(query)
	}
	return nil
}

// Fields returns the current fields description for the query
func (conn *Connection) Fields() (fields []*querypb.Field, err error) {
	nfields := int(conn.c.num_fields)
	if nfields == 0 {
		return nil, nil
	}
	cfields := (*[maxSize]C.MYSQL_FIELD)(unsafe.Pointer(conn.c.fields))
	totalLength := uint64(0)
	for i := 0; i < nfields; i++ {
		totalLength += uint64(cfields[i].name_length)
	}
	fields = make([]*querypb.Field, nfields)
	fvals := make([]querypb.Field, nfields)
	for i := 0; i < nfields; i++ {
		fvals[i].Name = copyString(cfields[i].name_length, cfields[i].name)
		fvals[i].OrgName = copyString(cfields[i].org_name_length, cfields[i].org_name)
		fvals[i].Table = copyString(cfields[i].table_length, cfields[i].table)
		fvals[i].OrgTable = copyString(cfields[i].org_table_length, cfields[i].org_table)
		fvals[i].Database = copyString(cfields[i].db_length, cfields[i].db)
		fvals[i].ColumnLength = uint32(cfields[i].length)
		fvals[i].Flags = uint32(cfields[i].flags)
		fvals[i].Charset = uint32(cfields[i].charsetnr)
		fvals[i].Decimals = uint32(cfields[i].decimals)
		fvals[i].Type, err = sqltypes.MySQLToType(int64(cfields[i]._type), int64(cfields[i].flags))

		if err != nil {
			return nil, err
		}
		fields[i] = &fvals[i]
	}
	return fields, nil
}

func copyString(length C.uint, field *C.char) string {
	return string(copyBytes(length, field))
}

func copyBytes(length C.uint, field *C.char) []byte {
	return (*[maxSize]byte)(unsafe.Pointer(field))[:length]
}

func (conn *Connection) fetchAll() (rows [][]sqltypes.Value, err error) {
	rowCount := int(conn.c.affected_rows)
	if rowCount == 0 {
		return nil, nil
	}
	rows = make([][]sqltypes.Value, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i], err = conn.FetchNext()
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// FetchNext returns the next row for a query
func (conn *Connection) FetchNext() (row []sqltypes.Value, err error) {
	vtrow := C.vt_fetch_next(&conn.c)
	if vtrow.has_error != 0 {
		return nil, conn.lastError("")
	}
	rowPtr := (*[maxSize]*[maxSize]byte)(unsafe.Pointer(vtrow.mysql_row))
	if rowPtr == nil {
		return nil, nil
	}
	colCount := int(conn.c.num_fields)
	cfields := (*[maxSize]C.MYSQL_FIELD)(unsafe.Pointer(conn.c.fields))
	row = make([]sqltypes.Value, colCount)
	lengths := (*[maxSize]uint64)(unsafe.Pointer(vtrow.lengths))
	totalLength := uint64(0)
	for i := 0; i < colCount; i++ {
		totalLength += lengths[i]
	}
	arena := make([]byte, 0, int(totalLength))
	for i := 0; i < colCount; i++ {
		colLength := lengths[i]
		colPtr := rowPtr[i]
		if colPtr == nil {
			continue
		}
		start := len(arena)
		arena = append(arena, colPtr[:colLength]...)
		typ, err := sqltypes.MySQLToType(int64(cfields[i]._type), int64(cfields[i].flags))
		if err != nil {
			return nil, err
		}
		// MySQL values can be trusted.
		row[i] = sqltypes.MakeTrusted(
			typ,
			arena[start:start+int(colLength)],
		)
	}
	return row, nil
}

// CloseResult finishes the result set
func (conn *Connection) CloseResult() {
	C.vt_close_result(&conn.c)
}

// ID returns the MySQL thread_id of the connection.
func (conn *Connection) ID() int64 {
	if conn.c.mysql == nil {
		return 0
	}
	return int64(C.vt_thread_id(&conn.c))
}

func (conn *Connection) lastError(query string) error {
	if err := C.vt_error(&conn.c); *err != 0 {
		return &sqldb.SQLError{
			Num:     int(C.vt_errno(&conn.c)),
			State:   C.GoString(C.vt_sqlstate(&conn.c)),
			Message: C.GoString(err),
			Query:   query,
		}
	}
	return &sqldb.SQLError{
		Num:     0,
		State:   sqldb.SQLStateGeneral,
		Message: "Dummy",
		Query:   string(query),
	}
}

func cfree(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}

// Make sure mysql.Connection implements sqldb.Conn
var _ (sqldb.Conn) = (*Connection)(nil)
