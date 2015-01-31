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
	"strconv"
	"unsafe"

	"github.com/youtube/vitess/go/hack"
	"github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
)

const (
	// NOTE(szopa): maxSize used to be 1 << 30, but that causes
	// compiler errors in some situations.
	maxSize = 1 << 20
)

func init() {
	// This needs to be called before threads begin to spawn.
	C.vt_library_init()
}

const (
	// ErrDupEntry is C.ER_DUP_ENTRY
	ErrDupEntry = C.ER_DUP_ENTRY

	// ErrLockWaitTimeout is C.ER_LOCK_WAIT_TIMEOUT
	ErrLockWaitTimeout = C.ER_LOCK_WAIT_TIMEOUT

	// ErrLockDeadlock is C.ER_LOCK_DEADLOCK
	ErrLockDeadlock = C.ER_LOCK_DEADLOCK

	// ErrOptionPreventsStatement is C.ER_OPTION_PREVENTS_STATEMENT
	ErrOptionPreventsStatement = C.ER_OPTION_PREVENTS_STATEMENT

	// RedactedPassword is the password value used in redacted configs
	RedactedPassword = "****"
)

// SqlError is the error structure returned from calling a mysql
// library function
type SqlError struct {
	Num     int
	Message string
	Query   string
}

// NewSqlError returns a new SqlError
func NewSqlError(number int, format string, args ...interface{}) *SqlError {
	return &SqlError{Num: number, Message: fmt.Sprintf(format, args...)}
}

// Error implements the error interface
func (se *SqlError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

// Number returns the internal mysql error code
func (se *SqlError) Number() int {
	return se.Num
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*SqlError)
		*err = terr
	}
}

// ConnectionParams contains all the parameters to use to connect to mysql
type ConnectionParams struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Uname      string `json:"uname"`
	Pass       string `json:"pass"`
	DbName     string `json:"dbname"`
	UnixSocket string `json:"unix_socket"`
	Charset    string `json:"charset"`
	Flags      uint64 `json:"flags"`

	// the following flags are only used for 'Change Master' command
	// for now (along with flags |= 2048 for CLIENT_SSL)
	SslCa     string `json:"ssl_ca"`
	SslCaPath string `json:"ssl_ca_path"`
	SslCert   string `json:"ssl_cert"`
	SslKey    string `json:"ssl_key"`
}

// EnableMultiStatements will set the right flag on the parameters
func (c *ConnectionParams) EnableMultiStatements() {
	c.Flags |= C.CLIENT_MULTI_STATEMENTS
}

// EnableSSL will set the right flag on the parameters
func (c *ConnectionParams) EnableSSL() {
	c.Flags |= C.CLIENT_SSL
}

// SslEnabled returns if SSL is enabled
func (c *ConnectionParams) SslEnabled() bool {
	return (c.Flags & C.CLIENT_SSL) != 0
}

// Redact will alter the ConnectionParams so they can be displayed
func (c *ConnectionParams) Redact() {
	c.Pass = RedactedPassword
}

// Connection encapsulates a C mysql library connection
type Connection struct {
	c C.VT_CONN
}

// Connect uses the connection parameters to connect and returns the connection
func Connect(params ConnectionParams) (conn *Connection, err error) {
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

	conn = &Connection{}
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
func (conn *Connection) ExecuteFetch(query string, maxrows int, wantfields bool) (qr *proto.QueryResult, err error) {
	if conn.IsClosed() {
		return nil, NewSqlError(2006, "Connection is closed")
	}

	if C.vt_execute(&conn.c, (*C.char)(hack.StringPointer(query)), C.ulong(len(query)), 0) != 0 {
		return nil, conn.lastError(query)
	}
	defer conn.CloseResult()

	qr = &proto.QueryResult{}
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

// ExecuteFetchMap returns a map from column names to cell data for a query
// that should return exactly 1 row.
func (conn *Connection) ExecuteFetchMap(query string) (map[string]string, error) {
	qr, err := conn.ExecuteFetch(query, 1, true)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("query %#v returned %d rows, expected 1", query, len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, fmt.Errorf("query %#v returned %d column names, expected %d", query, len(qr.Fields), len(qr.Rows[0]))
	}

	rowMap := make(map[string]string)
	for i, value := range qr.Rows[0] {
		rowMap[qr.Fields[i].Name] = value.String()
	}
	return rowMap, nil
}

// ExecuteStreamFetch starts a streaming query to mysql. Use FetchNext
// on the Connection until it returns nil or error
func (conn *Connection) ExecuteStreamFetch(query string) (err error) {
	if conn.IsClosed() {
		return NewSqlError(2006, "Connection is closed")
	}
	if C.vt_execute(&conn.c, (*C.char)(hack.StringPointer(query)), C.ulong(len(query)), 1) != 0 {
		return conn.lastError(query)
	}
	return nil
}

// Fields returns the current fields description for the query
func (conn *Connection) Fields() (fields []proto.Field) {
	nfields := int(conn.c.num_fields)
	if nfields == 0 {
		return nil
	}
	cfields := (*[maxSize]C.MYSQL_FIELD)(unsafe.Pointer(conn.c.fields))
	totalLength := uint64(0)
	for i := 0; i < nfields; i++ {
		totalLength += uint64(cfields[i].name_length)
	}
	fields = make([]proto.Field, nfields)
	for i := 0; i < nfields; i++ {
		length := cfields[i].name_length
		fname := (*[maxSize]byte)(unsafe.Pointer(cfields[i].name))[:length]
		fields[i].Name = string(fname)
		fields[i].Type = int64(cfields[i]._type)
	}
	return fields
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
		row[i] = BuildValue(arena[start:start+int(colLength)], cfields[i]._type)
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
		return &SqlError{Num: int(C.vt_errno(&conn.c)), Message: C.GoString(err), Query: query}
	}
	return &SqlError{0, "Dummy", string(query)}
}

// ReadPacket reads a raw packet from the MySQL connection.
//
// A MySQL packet is "a single SQL statement sent to the MySQL server, a
// single row that is sent to the client, or a binary log event sent from a
// master replication server to a slave." -MySQL 5.1 Reference Manual
func (conn *Connection) ReadPacket() ([]byte, error) {
	length := C.vt_cli_safe_read(&conn.c)
	if length == 0 {
		return nil, conn.lastError("ReadPacket()")
	}

	return C.GoBytes(unsafe.Pointer(conn.c.mysql.net.read_pos), C.int(length)), nil
}

// SendCommand sends a raw command to the MySQL server.
func (conn *Connection) SendCommand(command uint32, data []byte) error {
	var ret C.my_bool
	if data == nil {
		ret = C.vt_simple_command(&conn.c, command, nil, 0, 1)
	} else {
		ret = C.vt_simple_command(&conn.c, command, (*C.uchar)(unsafe.Pointer(&data[0])), C.ulong(len(data)), 1)
	}
	if ret != 0 {
		return conn.lastError(fmt.Sprintf("SendCommand(%#v, %#v)", command, data))
	}
	return nil
}

// Shutdown invokes the low-level shutdown call on the socket associated with
// a MySQL connection to stop ongoing communication. This is necessary when a
// thread is blocked in a MySQL I/O call, such as  ReadPacket(), and another
// thread wants to cancel the operation. We can't use mysql_close() because it
// isn't thread-safe.
func (conn *Connection) Shutdown() {
	C.vt_shutdown(&conn.c)
}

// GetCharset returns the current numerical values of the per-session character
// set variables.
func (conn *Connection) GetCharset() (cs proto.Charset, err error) {
	// character_set_client
	row, err := conn.ExecuteFetchMap("SHOW COLLATION WHERE `charset`=@@session.character_set_client AND `default`='Yes'")
	if err != nil {
		return cs, err
	}
	i, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return cs, err
	}
	cs.Client = int(i)

	// collation_connection
	row, err = conn.ExecuteFetchMap("SHOW COLLATION WHERE `collation`=@@session.collation_connection")
	if err != nil {
		return cs, err
	}
	i, err = strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return cs, err
	}
	cs.Conn = int(i)

	// collation_server
	row, err = conn.ExecuteFetchMap("SHOW COLLATION WHERE `collation`=@@session.collation_server")
	if err != nil {
		return cs, err
	}
	i, err = strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return cs, err
	}
	cs.Server = int(i)

	return cs, nil
}

// SetCharset changes the per-session character set variables.
func (conn *Connection) SetCharset(cs proto.Charset) error {
	sql := fmt.Sprintf(
		"SET @@session.character_set_client=%d, @@session.collation_connection=%d, @@session.collation_server=%d",
		cs.Client, cs.Conn, cs.Server)
	_, err := conn.ExecuteFetch(sql, 1, false)
	return err
}

// BuildValue returns a sqltypes.Value from the passed in fields
func BuildValue(bytes []byte, fieldType uint32) sqltypes.Value {
	if bytes == nil {
		return sqltypes.NULL
	}
	switch fieldType {
	case C.MYSQL_TYPE_DECIMAL, C.MYSQL_TYPE_FLOAT, C.MYSQL_TYPE_DOUBLE, C.MYSQL_TYPE_NEWDECIMAL:
		return sqltypes.MakeFractional(bytes)
	case C.MYSQL_TYPE_TIMESTAMP:
		return sqltypes.MakeString(bytes)
	}
	// The below condition represents the following list of values:
	// C.MYSQL_TYPE_TINY, C.MYSQL_TYPE_SHORT, C.MYSQL_TYPE_LONG, C.MYSQL_TYPE_LONGLONG, C.MYSQL_TYPE_INT24, C.MYSQL_TYPE_YEAR:
	if fieldType <= C.MYSQL_TYPE_INT24 || fieldType == C.MYSQL_TYPE_YEAR {
		return sqltypes.MakeNumeric(bytes)
	}
	return sqltypes.MakeString(bytes)
}

func cfree(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}
