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
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

const (
	// NOTE(szopa): maxSize used to be 1 << 30, but that causes
	// compiler errors in some situations.
	maxSize = 1 << 20
)

func init() {
	// This needs to be called before threads begin to spawn.
	C.vt_library_init()
	sqldb.RegisterDefault(Connect)
}

const (
	// typeDecimal is a deprecated type.
	// Value is 0.
	typeDecimal = C.MYSQL_TYPE_DECIMAL
	// TypeTiny specifies a TINYINT type.
	// Value is 1.
	TypeTiny = C.MYSQL_TYPE_TINY
	// TypeShort specifies a SMALLINT type.
	// Value is 2.
	TypeShort = C.MYSQL_TYPE_SHORT
	// TypeLong specifies a INTEGER type.
	// Value is 3.
	TypeLong = C.MYSQL_TYPE_LONG
	// TypeFloat specifies a FLOAT type.
	// Value is 4.
	TypeFloat = C.MYSQL_TYPE_FLOAT
	// TypeDouble specifies a DOUBLE or REAL type.
	// Value is 5.
	TypeDouble = C.MYSQL_TYPE_DOUBLE
	// TypeNull specifies a NULL type.
	// Value is 6.
	TypeNull = C.MYSQL_TYPE_NULL
	// TypeTimestamp specifies a TIMESTAMP type.
	// Value is 7. NOT SUPPORTED.
	TypeTimestamp = C.MYSQL_TYPE_TIMESTAMP
	// TypeLonglong specifies a BIGINT type.
	// Value is 8.
	TypeLonglong = C.MYSQL_TYPE_LONGLONG
	// TypeInt24 specifies a MEDIUMINT type.
	// Value is 9.
	TypeInt24 = C.MYSQL_TYPE_INT24
	// TypeDate specifies a DATE type.
	// Value is 10.
	TypeDate = C.MYSQL_TYPE_DATE
	// TypeTime specifies a TIME type.
	// Value is 11.
	TypeTime = C.MYSQL_TYPE_TIME
	// TypeDatetime specifies a DATETIME type.
	// Value is 12.
	TypeDatetime = C.MYSQL_TYPE_DATETIME
	// TypeYear specifies a YEAR type.
	// Value is 13.
	TypeYear = C.MYSQL_TYPE_YEAR
	// TypeBit specifies a BIT type.
	// Value is 16.
	TypeBit = C.MYSQL_TYPE_BIT
	// TypeNewDecimal specifies a DECIMAL or NUMERIC type.
	// Value is 246.
	TypeNewDecimal = C.MYSQL_TYPE_NEWDECIMAL
	// TypeBlob specifies a BLOB or TEXT type.
	// Value is 252.
	TypeBlob = C.MYSQL_TYPE_BLOB
	// TypeVarString specifies a VARCHAR or VARBINARY type.
	// Value is 253.
	TypeVarString = C.MYSQL_TYPE_VAR_STRING
	// TypeString specifies a CHAR or BINARY type.
	// Value is 254.
	TypeString = C.MYSQL_TYPE_STRING
	// TypeGeometry specifies a Spatial field.
	// Value is 255. NOT SUPPORTED.
	TypeGeometry = C.MYSQL_TYPE_GEOMETRY
)

const (
	// FlagUnsigned specifies if the value is an unsigned.
	// Value is 32 (0x20).
	FlagUnsigned = C.UNSIGNED_FLAG
	// FlagBinary specifies if the data is binary.
	// Value is 128 (0x80).
	FlagBinary = C.BINARY_FLAG
	// FlagEnum specifies if the value is an enum.
	// Value is 256 (0x100).
	FlagEnum = C.ENUM_FLAG
	// FlagSet specifies if the value is a set.
	// Value is 2048 (0x800).
	FlagSet = C.SET_FLAG

	// RelevantFlags is used to mask out irrelevant flags.
	RelevantFlags = FlagUnsigned |
		FlagBinary |
		FlagEnum |
		FlagSet
)

const (
	// ErrDupEntry is C.ER_DUP_ENTRY
	ErrDupEntry = C.ER_DUP_ENTRY

	// ErrLockWaitTimeout is C.ER_LOCK_WAIT_TIMEOUT
	ErrLockWaitTimeout = C.ER_LOCK_WAIT_TIMEOUT

	// ErrLockDeadlock is C.ER_LOCK_DEADLOCK
	ErrLockDeadlock = C.ER_LOCK_DEADLOCK

	// ErrOptionPreventsStatement is C.ER_OPTION_PREVENTS_STATEMENT
	ErrOptionPreventsStatement = C.ER_OPTION_PREVENTS_STATEMENT

	// ErrDataTooLong is C.ER_DATA_TOO_LONG
	ErrDataTooLong = C.ER_DATA_TOO_LONG

	// ErrBadNullError is C.ER_BAD_NULL_ERROR
	ErrBadNullError = C.ER_BAD_NULL_ERROR

	// ErrDataOutOfRange is C.ER_WARN_DATA_OUT_OF_RANGE
	ErrDataOutOfRange = C.ER_WARN_DATA_OUT_OF_RANGE

	// ErrServerLost is C.CR_SERVER_LOST.
	// It's hard-coded for now because it causes problems on import.
	ErrServerLost = 2013

	// RedactedPassword is the password value used in redacted configs
	RedactedPassword = "****"
)

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*sqldb.SQLError)
		*err = terr
	}
}

// EnableSSL will set the right flag on the parameters
func EnableSSL(connParams *sqldb.ConnParams) {
	connParams.Flags |= C.CLIENT_SSL
}

// SslEnabled returns if SSL is enabled
func SslEnabled(connParams *sqldb.ConnParams) bool {
	return (connParams.Flags & C.CLIENT_SSL) != 0
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
		return nil, sqldb.NewSQLError(2006, "Connection is closed")
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
			Query:   string(query),
		}
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
		return sqldb.NewSQLError(2006, "Connection is closed")
	}
	if C.vt_execute(&conn.c, (*C.char)(hack.StringPointer(query)), C.ulong(len(query)), 1) != 0 {
		return conn.lastError(query)
	}
	return nil
}

// Fields returns the current fields description for the query
func (conn *Connection) Fields() (fields []*querypb.Field) {
	nfields := int(conn.c.num_fields)
	if nfields == 0 {
		return nil
	}
	cfields := (*[maxSize]C.MYSQL_FIELD)(unsafe.Pointer(conn.c.fields))
	totalLength := uint64(0)
	for i := 0; i < nfields; i++ {
		totalLength += uint64(cfields[i].name_length)
	}
	fields = make([]*querypb.Field, nfields)
	fvals := make([]querypb.Field, nfields)
	for i := 0; i < nfields; i++ {
		length := cfields[i].name_length
		fname := (*[maxSize]byte)(unsafe.Pointer(cfields[i].name))[:length]
		fvals[i].Name = string(fname)
		fvals[i].Type = sqltypes.MySQLToType(int64(cfields[i]._type), int64(cfields[i].flags))
		fields[i] = &fvals[i]
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
		// MySQL values can be trusted.
		row[i] = sqltypes.MakeTrusted(
			sqltypes.MySQLToType(int64(cfields[i]._type), int64(cfields[i].flags)),
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
			Message: C.GoString(err),
			Query:   query,
		}
	}
	return &sqldb.SQLError{
		Num:     0,
		Message: "Dummy",
		Query:   string(query),
	}
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
func (conn *Connection) GetCharset() (*binlogdatapb.Charset, error) {
	// character_set_client
	row, err := conn.ExecuteFetchMap("SHOW COLLATION WHERE `charset`=@@session.character_set_client AND `default`='Yes'")
	if err != nil {
		return nil, err
	}
	client, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	// collation_connection
	row, err = conn.ExecuteFetchMap("SHOW COLLATION WHERE `collation`=@@session.collation_connection")
	if err != nil {
		return nil, err
	}
	connection, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	// collation_server
	row, err = conn.ExecuteFetchMap("SHOW COLLATION WHERE `collation`=@@session.collation_server")
	if err != nil {
		return nil, err
	}
	server, err := strconv.ParseInt(row["Id"], 10, 16)
	if err != nil {
		return nil, err
	}

	return &binlogdatapb.Charset{
		Client: int32(client),
		Conn:   int32(connection),
		Server: int32(server),
	}, nil
}

// SetCharset changes the per-session character set variables.
func (conn *Connection) SetCharset(cs *binlogdatapb.Charset) error {
	sql := fmt.Sprintf(
		"SET @@session.character_set_client=%d, @@session.collation_connection=%d, @@session.collation_server=%d",
		cs.Client, cs.Conn, cs.Server)
	_, err := conn.ExecuteFetch(sql, 1, false)
	return err
}

func cfree(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}

// Make sure mysql.Connection implements sqldb.Conn
var _ (sqldb.Conn) = (*Connection)(nil)
