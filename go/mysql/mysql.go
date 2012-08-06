// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysql

/*
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

func (self *SqlError) Error() string {
	if self.Query == "" {
		return fmt.Sprintf("%v (errno %v)", self.Message, self.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", self.Message, self.Num, self.Query)
}

func (self *SqlError) Number() int {
	return self.Num
}

func handleError(err *error) {
	if x := recover(); x != nil {
		terr := x.(*SqlError)
		*err = terr
	}
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

func Connect(info map[string]interface{}) (conn *Connection, err error) {
	defer handleError(&err)

	host := mapCString(info, "host")
	defer cfree(host)
	port := mapint(info, "port")
	uname := mapCString(info, "uname")
	defer cfree(uname)
	pass := mapCString(info, "pass")
	defer cfree(pass)
	dbname := mapCString(info, "dbname")
	defer cfree(dbname)
	unix_socket := mapCString(info, "unix_socket")
	defer cfree(unix_socket)
	charset := mapCString(info, "charset")
	defer cfree(charset)

	conn = &Connection{}
	conn.handle = C.mysql_init(nil)
	if C.vt_mysql_real_connect(conn.handle, host, uname, pass, dbname, C.uint(port), unix_socket, 0) == nil {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}

	if C.vt_mysql_set_character_set(conn.handle, charset) != 0 {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}
	return conn, nil
}

func (self *Connection) ExecuteFetch(query []byte, maxrows int, wantfields bool) (qr *QueryResult, err error) {
	defer handleError(&err)
	self.validate()

	if C.vt_mysql_real_query(self.handle, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query))) != 0 {
		return nil, self.lastError(query)
	}

	result := C.vt_mysql_store_result(self.handle)
	if result == nil {
		if int(C.vt_mysql_field_count(self.handle)) != 0 { // Query was supposed to return data, but it didn't
			return nil, self.lastError(query)
		}
		qr = &QueryResult{}
		qr.RowsAffected = uint64(C.vt_mysql_affected_rows(self.handle))
		qr.InsertId = uint64(C.vt_mysql_insert_id(self.handle))
		return qr, nil
	}
	defer C.vt_mysql_free_result(result)
	qr = &QueryResult{}
	qr.RowsAffected = uint64(C.vt_mysql_affected_rows(self.handle))
	if qr.RowsAffected > uint64(maxrows) {
		return nil, &SqlError{0, fmt.Sprintf("Row count exceeded %d", maxrows), string(query)}
	}
	if wantfields {
		qr.Fields = self.buildFields(result)
	}
	qr.Rows = self.fetchAll(result)
	return qr, nil
}

func (self *Connection) Id() int64 {
	if self.handle == nil {
		return 0
	}
	return int64(C.vt_mysql_thread_id(self.handle))
}

func (self *Connection) Close() {
	if self.handle == nil {
		return
	}
	C.vt_mysql_close(self.handle)
	self.handle = nil
}

func (self *Connection) IsClosed() bool {
	return self.handle == nil
}

func (self *Connection) buildFields(result *C.MYSQL_RES) (fields []Field) {
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

func (self *Connection) fetchAll(result *C.MYSQL_RES) (rows [][]interface{}) {
	rowCount := int(C.vt_mysql_num_rows(result))
	if rowCount == 0 {
		return nil
	}
	rows = make([][]interface{}, rowCount)
	colCount := int(C.vt_mysql_num_fields(result))
	for i := 0; i < rowCount; i++ {
		rows[i] = self.fetchNext(result, colCount)
	}
	return rows
}

func (self *Connection) fetchNext(result *C.MYSQL_RES, colCount int) (row []interface{}) {
	rowPtr := (*[1 << 30]*[1 << 30]byte)(unsafe.Pointer(C.vt_mysql_fetch_row(result)))
	if rowPtr == nil {
		panic(self.lastError(nil))
	}
	row = make([]interface{}, colCount)
	lengths := (*[1 << 30]uint64)(unsafe.Pointer(C.vt_mysql_fetch_lengths(result)))
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

func (self *Connection) lastError(query []byte) error {
	if err := C.vt_mysql_error(self.handle); *err != 0 {
		return &SqlError{Num: int(C.vt_mysql_errno(self.handle)), Message: C.GoString(err), Query: string(query)}
	}
	return &SqlError{0, "Dummy", string(query)}
}

func (self *Connection) validate() {
	if self.handle == nil {
		panic(NewSqlError(2006, "Connection is closed"))
	}
}

func mapCString(info map[string]interface{}, key string) *C.char {
	ival, ok := info[key]
	if !ok {
		ival = ""
	}
	sval, ok := ival.(string)
	if !ok {
		panic(NewSqlError(0, "Expecting string for %s, received %T", key, ival))
	}
	if sval == "" {
		return nil
	}
	return C.CString(sval)
}

func mapint(info map[string]interface{}, key string) int {
	ival, ok := info[key]
	if !ok {
		ival = int(0)
	}
	intval, ok := ival.(int)
	if !ok {
		panic(NewSqlError(0, "Expecting int for %s, received %T", key, ival))
	}
	return intval
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
