// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysql

/*
#cgo pkg-config: gomysql
#include <stdlib.h>
#include <mysql.h>

// The vt_mysql_* functions are needed to honor the mysql client library's
// assumption that all threads using mysql functions have first called
// my_thread_init().
//
// Any goroutine may run on any thread at any time. However, a Cgo call
// is guaranteed to run on a single thread for its duration.  Therefore,
// calling my_thread_init() before every actual operation is sufficient.
// Multiple calls to my_thread_init() are guarded interanlly in mysql and
// the call is cheap.

MYSQL* vt_mysql_real_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag) {
  my_thread_init();
  return mysql_real_connect(mysql, host, user, passwd, db, port, unix_socket, client_flag);
}

int vt_mysql_set_character_set(MYSQL *mysql, const char *csname) {
  my_thread_init();
  return mysql_set_character_set(mysql, csname);
}

int vt_mysql_real_query(MYSQL *mysql, const char *stmt_str, unsigned long length) {
  my_thread_init();
  return mysql_real_query(mysql, stmt_str, length);
}

MYSQL_RES *vt_mysql_store_result(MYSQL *mysql) {
  my_thread_init();
  return mysql_store_result(mysql);
}

MYSQL_RES *vt_mysql_use_result(MYSQL *mysql) {
  my_thread_init();
  return mysql_use_result(mysql);
}

unsigned int vt_mysql_field_count(MYSQL *mysql) {
  my_thread_init();
  return mysql_field_count(mysql);
}

my_ulonglong vt_mysql_affected_rows(MYSQL *mysql) {
  my_thread_init();
  return mysql_affected_rows(mysql);
}

my_ulonglong vt_mysql_insert_id(MYSQL *mysql) {
  my_thread_init();
  return mysql_insert_id(mysql);
}

void vt_mysql_free_result(MYSQL_RES *result) {
  my_thread_init();
  mysql_free_result(result);
}

unsigned long vt_mysql_thread_id(MYSQL *mysql) {
  my_thread_init();
  return mysql_thread_id(mysql);
}

void vt_mysql_close(MYSQL *mysql) {
  my_thread_init();
  mysql_close(mysql);
}

MYSQL_FIELD *vt_mysql_fetch_fields(MYSQL_RES *result) {
  my_thread_init();
  return mysql_fetch_fields(result);
}

unsigned int vt_mysql_num_fields(MYSQL_RES *result) {
  my_thread_init();
  return mysql_num_fields(result);
}

my_ulonglong vt_mysql_num_rows(MYSQL_RES *result) {
  my_thread_init();
  return mysql_num_rows(result);
}

MYSQL_ROW vt_mysql_fetch_row(MYSQL_RES *result) {
  my_thread_init();
  return mysql_fetch_row(result);
}

unsigned long *vt_mysql_fetch_lengths(MYSQL_RES *result) {
  my_thread_init();
  return mysql_fetch_lengths(result);
}

unsigned int vt_mysql_errno(MYSQL *mysql) {
  my_thread_init();
  return mysql_errno(mysql);
}

const char *vt_mysql_error(MYSQL *mysql) {
  my_thread_init();
  return mysql_error(mysql);
}
*/
import "C"

import (
	"code.google.com/p/vitess/go/hack"
	"fmt"
	"unsafe"
)

func init() {
	// This needs to be called before threads begin to spawn.
	C.mysql_library_init(0, nil, nil)
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
	handle *C.MYSQL
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
	conn.handle = C.mysql_init(nil)
	if C.vt_mysql_real_connect(conn.handle, host, uname, pass, dbname, port, unix_socket, 0) == nil {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}

	if C.vt_mysql_set_character_set(conn.handle, charset) != 0 {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}
	return conn, nil
}

func (conn *Connection) ExecuteFetch(query []byte, maxrows int, wantfields bool) (qr *QueryResult, err error) {
	defer handleError(&err)
	conn.validate()

	if C.vt_mysql_real_query(conn.handle, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query))) != 0 {
		return nil, conn.lastError(query)
	}

	result := C.vt_mysql_store_result(conn.handle)
	if result == nil {
		if int(C.vt_mysql_field_count(conn.handle)) != 0 { // Query was supposed to return data, but it didn't
			return nil, conn.lastError(query)
		}
		qr = &QueryResult{}
		qr.RowsAffected = uint64(C.vt_mysql_affected_rows(conn.handle))
		qr.InsertId = uint64(C.vt_mysql_insert_id(conn.handle))
		return qr, nil
	}
	r := &Result{result}
	defer C.vt_mysql_free_result(result)
	qr = &QueryResult{}
	qr.RowsAffected = uint64(C.vt_mysql_affected_rows(conn.handle))
	if qr.RowsAffected > uint64(maxrows) {
		return nil, &SqlError{0, fmt.Sprintf("Row count exceeded %d", maxrows), string(query)}
	}
	if wantfields {
		qr.Fields = conn.buildFields(result)
	}
	qr.Rows = r.fetchAll(conn)
	return qr, nil
}

// when using ExecuteStreamFetch, use FetchNext on the Result
// if FetchNext returns nil, use LastErrorOrNil on the Connection
// to see if it was planned or not
func (conn *Connection) ExecuteStreamFetch(query []byte) (result *Result, fields []Field, err error) {
	defer handleError(&err)
	conn.validate()

	if C.vt_mysql_real_query(conn.handle, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query))) != 0 {
		return nil, nil, conn.lastError(query)
	}

	r := C.vt_mysql_use_result(conn.handle)
	if r == nil {
		if int(C.vt_mysql_field_count(conn.handle)) != 0 { // Query was supposed to return data, but it didn't
			return nil, nil, conn.lastError(query)
		}
		return nil, nil, nil
	}

	return &Result{r}, conn.buildFields(r), nil
}

func (conn *Connection) Id() int64 {
	if conn.handle == nil {
		return 0
	}
	return int64(C.vt_mysql_thread_id(conn.handle))
}

func (conn *Connection) Close() {
	if conn.handle == nil {
		return
	}
	C.vt_mysql_close(conn.handle)
	conn.handle = nil
}

func (conn *Connection) IsClosed() bool {
	return conn.handle == nil
}

func (conn *Connection) buildFields(result *C.MYSQL_RES) (fields []Field) {
	nfields := int(C.vt_mysql_num_fields(result))
	cfieldsptr := C.vt_mysql_fetch_fields(result)
	cfields := (*[1 << 30]C.MYSQL_FIELD)(unsafe.Pointer(cfieldsptr))
	arena := hack.NewStringArena(1024) // prealloc a reasonable amount of space
	fields = make([]Field, nfields)
	for i := 0; i < nfields; i++ {
		length := strlen(cfields[i].name)
		fname := (*[1 << 30]byte)(unsafe.Pointer(cfields[i].name))[:length]
		fields[i].Name = arena.NewString(fname)
		fields[i].Type = int64(cfields[i]._type)
	}
	return fields
}

func (conn *Connection) lastError(query []byte) error {
	if err := C.vt_mysql_error(conn.handle); *err != 0 {
		return &SqlError{Num: int(C.vt_mysql_errno(conn.handle)), Message: C.GoString(err), Query: string(query)}
	}
	return &SqlError{0, "Dummy", string(query)}
}

func (conn *Connection) LastErrorOrNil(query []byte) error {
	if err := C.vt_mysql_error(conn.handle); *err != 0 {
		return &SqlError{Num: int(C.vt_mysql_errno(conn.handle)), Message: C.GoString(err), Query: string(query)}
	}
	return nil
}

func (conn *Connection) validate() {
	if conn.handle == nil {
		panic(NewSqlError(2006, "Connection is closed"))
	}
}

type Result struct {
	myres *C.MYSQL_RES
}

func (result *Result) fetchAll(conn *Connection) (rows [][]interface{}) {
	rowCount := int(C.vt_mysql_num_rows(result.myres))
	if rowCount == 0 {
		return nil
	}
	rows = make([][]interface{}, rowCount)
	colCount := int(C.vt_mysql_num_fields(result.myres))
	for i := 0; i < rowCount; i++ {
		rows[i] = result.FetchNext(colCount)
		if rows[i] == nil {
			panic(conn.lastError(nil))
		}
	}
	return rows
}

func (result *Result) FetchNext(colCount int) (row []interface{}) {
	rowPtr := (*[1 << 30]*[1 << 30]byte)(unsafe.Pointer(C.vt_mysql_fetch_row(result.myres)))
	if rowPtr == nil {
		return nil
	}
	row = make([]interface{}, colCount)
	lengths := (*[1 << 30]uint64)(unsafe.Pointer(C.vt_mysql_fetch_lengths(result.myres)))
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
	return row
}

func (result *Result) Close() {
	C.vt_mysql_free_result(result.myres)
}

func cfree(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}

// Muahahaha
func strlen(str *C.char) int {
	gstr := (*[1 << 30]byte)(unsafe.Pointer(str))
	length := 0
	for gstr[length] != 0 {
		length++
	}
	return length
}
