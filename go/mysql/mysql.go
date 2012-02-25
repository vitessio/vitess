/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package mysql

// #include <stdlib.h>
// #include <mysql.h>
import "C"

import (
	"fmt"
	"vitess/hack"
	"unsafe"
)

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
	if C.mysql_real_connect(conn.handle, host, uname, pass, dbname, C.uint(port), unix_socket, 0) == nil {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}

	if C.mysql_set_character_set(conn.handle, charset) != 0 {
		defer conn.Close()
		return nil, conn.lastError(nil)
	}
	return conn, nil
}

func (self *Connection) ExecuteFetch(query []byte, maxrows int) (qr *QueryResult, err error) {
	defer handleError(&err)
	self.validate()

	if C.mysql_real_query(self.handle, (*C.char)(unsafe.Pointer(&query[0])), C.ulong(len(query))) != 0 {
		return nil, self.lastError(query)
	}

	result := C.mysql_store_result(self.handle)
	if result == nil {
		if int(C.mysql_field_count(self.handle)) != 0 { // Query was supposed to return data, but it didn't
			return nil, self.lastError(query)
		}
		qr = &QueryResult{}
		qr.RowsAffected = uint64(C.mysql_affected_rows(self.handle))
		qr.InsertId = uint64(C.mysql_insert_id(self.handle))
		return qr, nil
	}
	defer C.mysql_free_result(result)
	qr = &QueryResult{}
	qr.RowsAffected = uint64(C.mysql_affected_rows(self.handle))
	if qr.RowsAffected > uint64(maxrows) {
		return nil, &SqlError{0, fmt.Sprintf("Row count exceeded %d", maxrows), string(query)}
	}
	qr.Fields = self.buildFields(result)
	qr.Rows = self.fetchAll(result)
	return qr, nil
}

func (self *Connection) Id() int64 {
	if self.handle == nil {
		return 0
	}
	return int64(C.mysql_thread_id(self.handle))
}

func (self *Connection) Close() {
	if self.handle == nil {
		return
	}
	C.mysql_close(self.handle)
	self.handle = nil
}

func (self *Connection) buildFields(result *C.MYSQL_RES) (fields []Field) {
	nfields := int(C.mysql_num_fields(result))
	cfieldsptr := C.mysql_fetch_fields(result)
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
	rowCount := int(C.mysql_num_rows(result))
	if rowCount == 0 {
		return nil
	}
	rows = make([][]interface{}, rowCount)
	colCount := int(C.mysql_num_fields(result))
	for i := 0; i < rowCount; i++ {
		rows[i] = self.fetchNext(result, colCount)
	}
	return rows
}

func (self *Connection) fetchNext(result *C.MYSQL_RES, colCount int) (row []interface{}) {
	rowPtr := (*[1 << 30]*[1 << 30]byte)(unsafe.Pointer(C.mysql_fetch_row(result)))
	if rowPtr == nil {
		panic(self.lastError(nil))
	}
	row = make([]interface{}, colCount)
	lengths := (*[1 << 30]uint64)(unsafe.Pointer(C.mysql_fetch_lengths(result)))
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
	if err := C.mysql_error(self.handle); *err != 0 {
		return &SqlError{Num: int(C.mysql_errno(self.handle)), Message: C.GoString(err), Query: string(query)}
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
		panic(NewSqlError(0, "Missing connection parameter %s", key))
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
		panic(NewSqlError(0, "Missing connection parameter %s", key))
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
