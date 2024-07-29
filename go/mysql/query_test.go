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

package mysql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/dolthub/vitess/go/sqltypes"

	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// Utility function to write sql query as packets to test parseComPrepare
func MockQueryPackets(t *testing.T, query string) []byte {
	data := make([]byte, len(query)+1)
	// Not sure if it makes a difference
	pos := 0
	pos = writeByte(data, pos, ComPrepare)
	copy(data[pos:], query)
	return data
}

func MockPrepareData(t *testing.T) (*PrepareData, *sqltypes.Result) {
	sql := "select * from test_table where id = ?"

	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
			},
		},
		RowsAffected: 1,
	}

	prepare := &PrepareData{
		StatementID: 18,
		PrepareStmt: sql,
		ParamsCount: 1,
		ParamsType:  []int32{263},
		ColumnNames: []string{"id"},
		BindVars: map[string]*querypb.BindVariable{
			"v1": sqltypes.Int32BindVariable(10),
		},
	}

	return prepare, result
}

func TestComInitDB(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComInitDB packet, read it, compare.
	if err := cConn.writeComInitDB("my_db"); err != nil {
		t.Fatalf("writeComInitDB failed: %v", err)
	}
	data, err := sConn.ReadPacket(context.Background())
	if err != nil || len(data) == 0 || data[0] != ComInitDB {
		t.Fatalf("sConn.ReadPacket - ComInitDB failed: %v %v", data, err)
	}
	db := sConn.parseComInitDB(data)
	if db != "my_db" {
		t.Errorf("parseComInitDB returned unexpected data: %v", db)
	}
}

func TestComSetOption(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComSetOption packet, read it, compare.
	if err := cConn.writeComSetOption(1); err != nil {
		t.Fatalf("writeComSetOption failed: %v", err)
	}
	data, err := sConn.ReadPacket(context.Background())
	if err != nil || len(data) == 0 || data[0] != ComSetOption {
		t.Fatalf("sConn.ReadPacket - ComSetOption failed: %v %v", data, err)
	}
	operation, ok := sConn.parseComSetOption(data)
	if !ok {
		t.Fatalf("parseComSetOption failed unexpectedly")
	}
	if operation != 1 {
		t.Errorf("parseComSetOption returned unexpected data: %v", operation)
	}
}

func TestComStmtPrepare(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "select * from test_table where id = ?"
	mockData := MockQueryPackets(t, sql)

	if err := cConn.writePacket(mockData); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}

	data, err := sConn.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComPrepare failed: %v", err)
	}

	parsedQuery := sConn.parseComPrepare(data)
	if parsedQuery != sql {
		t.Fatalf("Received incorrect query, want: %v, got: %v", sql, parsedQuery)
	}

	prepare, result := MockPrepareData(t)
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// write the response to the client
	if err := sConn.writePrepare(context.Background(), result.Fields, prepare); err != nil {
		t.Fatalf("sConn.writePrepare failed: %v", err)
	}

	resp, err := cConn.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("cConn.ReadPacket failed: %v", err)
	}
	if uint32(resp[1]) != prepare.StatementID {
		t.Fatalf("Received incorrect Statement ID, want: %v, got: %v", prepare.StatementID, resp[1])
	}
}

func TestComStmtSendLongData(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, result := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare
	if err := cConn.writePrepare(context.Background(), result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtSendLongData, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket(context.Background())
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, paramID, chunkData, ok := sConn.parseComStmtSendLongData(data)
	if !ok {
		t.Fatalf("parseComStmtSendLongData failed")
	}
	if paramID != 1 {
		t.Fatalf("Received incorrect ParamID, want %v, got %v:", paramID, 1)
	}
	if stmtID != prepare.StatementID {
		t.Fatalf("Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)
	}
	// Check length of chunkData, Since its a subset of `data` and compare with it after we subtract the number of bytes that was read from it.
	// sizeof(uint32) + sizeof(uint16) + 1 = 7
	if len(chunkData) != len(data)-7 {
		t.Fatalf("Received bad chunkData")
	}
}

func TestComStmtExecute(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, _ := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare

	// This is simulated packets for `select * from test_table where id = ?`
	data := []byte{23, 18, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	stmtID, _, err := sConn.parseComStmtExecute(cConn.PrepareData, data)
	if err != nil {
		t.Fatalf("parseComStmtExeute failed: %v", err)
	}
	if stmtID != 18 {
		t.Fatalf("Parsed incorrect values")
	}
}

func TestComStmtExecuteNewParams(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	paramTypes := []querypb.Type{
		querypb.Type_DECIMAL,
		querypb.Type_INT8,
		querypb.Type_INT16,
		querypb.Type_INT32,
		querypb.Type_FLOAT32,
		querypb.Type_FLOAT64,
		querypb.Type_TIMESTAMP,
		querypb.Type_INT64,
		querypb.Type_INT24,
		querypb.Type_DATE,
		querypb.Type_TIME,
		querypb.Type_DATETIME,
		querypb.Type_YEAR,
		querypb.Type_VARCHAR,
		querypb.Type_BIT,
		querypb.Type_TIMESTAMP,
		querypb.Type_DATETIME,
		querypb.Type_TIME,
		querypb.Type_JSON,
		querypb.Type_DECIMAL,
		querypb.Type_ENUM,
		querypb.Type_SET,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_VARCHAR,
		querypb.Type_CHAR,
		querypb.Type_GEOMETRY,

		querypb.Type_DECIMAL,
		querypb.Type_UINT8,
		querypb.Type_UINT16,
		querypb.Type_UINT32,
		querypb.Type_FLOAT32,
		querypb.Type_FLOAT64,
		querypb.Type_TIMESTAMP,
		querypb.Type_UINT64,
		querypb.Type_UINT24,
		querypb.Type_DATE,
		querypb.Type_TIME,
		querypb.Type_DATETIME,
		querypb.Type_YEAR,
		querypb.Type_VARCHAR,
		querypb.Type_BIT,
		querypb.Type_TIMESTAMP,
		querypb.Type_DATETIME,
		querypb.Type_TIME,
		querypb.Type_JSON,
		querypb.Type_DECIMAL,
		querypb.Type_ENUM,
		querypb.Type_SET,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_TEXT,
		querypb.Type_VARCHAR,
		querypb.Type_CHAR,
		querypb.Type_GEOMETRY,
	}
	paramsCount := uint16(58)
	prepare := &PrepareData{
		StatementID: 123,
		ParamsCount: paramsCount, // TODO: figure out total number of param types
		ParamsType:  make([]int32, paramsCount),
		BindVars:    make(map[string]*querypb.BindVariable),
	}
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare

	data := []byte{
		23,           // status
		123, 0, 0, 0, // statement_id
		1,          // flags
		1, 0, 0, 0, // iteration_count
		0, 0, 0, 0, 0, 0, 0, 0, // null_bitmap
		1, // new_params_bind_flag

		0, 0, // parameter[0] DECIMAL, UNSIGNED
		1, 0, // parameter[1] INT8, UNSIGNED
		2, 0, // parameter[2] INT16, UNSIGNED
		3, 0, // parameter[3] INT32, UNSIGNED
		4, 0, // parameter[4] FLOAT32, UNSIGNED
		5, 0, // parameter[5] FLOAT64, UNSIGNED
		7, 0, // parameter[6] TIMESTAMP, UNSIGNED
		8, 0, // parameter[7] INT64, UNSIGNED
		9, 0, // parameter[8] INT24, UNSIGNED
		10, 0, // parameter[9] DATE, UNSIGNED
		11, 0, // parameter[10] TIME, UNSIGNED
		12, 0, // parameter[11] DATETIME, UNSIGNED
		13, 0, // parameter[12] YEAR, UNSIGNED
		15, 0, // parameter[13] VARCHAR, UNSIGNED
		16, 0, // parameter[14] BIT, UNSIGNED
		17, 0, // parameter[15] TIMESTAMP2, UNSIGNED
		18, 0, // parameter[16] DATETIME2, UNSIGNED
		19, 0, // parameter[17] TIME2, UNSIGNED
		245, 0, // parameter[18] JSON, UNSIGNED
		246, 0, // parameter[19] DECIMAL, UNSIGNED
		247, 0, // parameter[20] ENUM, UNSIGNED
		248, 0, // parameter[21] SET, UNSIGNED
		249, 0, // parameter[22] TINY_BLOB, UNSIGNED
		250, 0, // parameter[23] MEDIUM_BLOB, UNSIGNED
		251, 0, // parameter[24] LONG_BLOB, UNSIGNED
		252, 0, // parameter[25] BLOB, UNSIGNED
		253, 0, // parameter[26] VAR_CHAR, UNSIGNED
		254, 0, // parameter[27] CHAR, UNSIGNED
		255, 0, // parameter[28] GEOMETRY, UNSIGNED

		0, 128, // parameter[29] DECIMAL, SIGNED
		1, 128, // parameter[30] INT8, SIGNED
		2, 128, // parameter[31] INT16, SIGNED
		3, 128, // parameter[32] INT32, SIGNED
		4, 128, // parameter[33] FLOAT32, SIGNED
		5, 128, // parameter[34] FLOAT64, SIGNED
		7, 128, // parameter[35] TIMESTAMP, SIGNED
		8, 128, // parameter[36] INT64, SIGNED
		9, 128, // parameter[37] INT24, SIGNED
		10, 128, // parameter[38] DATE, SIGNED
		11, 128, // parameter[39] TIME, SIGNED
		12, 128, // parameter[40] DATETIME, SIGNED
		13, 128, // parameter[41] YEAR, SIGNED
		15, 128, // parameter[42] VARCHAR, SIGNED
		16, 128, // parameter[43] BIT, SIGNED
		17, 128, // parameter[44] TIMESTAMP2, SIGNED
		18, 128, // parameter[45] DATETIME2, SIGNED
		19, 128, // parameter[46] TIME2, SIGNED
		245, 128, // parameter[47] JSON, SIGNED
		246, 128, // parameter[48] DECIMAL, SIGNED
		247, 128, // parameter[49] ENUM, SIGNED
		248, 128, // parameter[50] SET, SIGNED
		249, 128, // parameter[51] TINY_BLOB, SIGNED
		250, 128, // parameter[52] MEDIUM_BLOB, SIGNED
		251, 128, // parameter[53] LONG_BLOB, SIGNED
		252, 128, // parameter[54] BLOB, SIGNED
		253, 128, // parameter[55] VAR_CHAR, SIGNED
		254, 128, // parameter[56] CHAR, SIGNED
		255, 128, // parameter[57] GEOMETRY, SIGNED

		0x03, 0x66, 0x6f, 0x6f, // parameter[0]  DECIMAL
		0x00,       // parameter[1]  INT8
		0x00, 0x00, // parameter[2]  INT16
		0x00, 0x00, 0x00, 0x00, // parameter[3]  INT32
		0x00, 0x00, 0x00, 0x00, // parameter[4]  FLOAT32
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // parameter[5]  FLOAT64
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[6]  TIMESTAMP
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, // parameter[7]  INT64
		0x0b, 0xda, 0x07, 0x0a, // parameter[8]  INT24
		0x04, 0xda, 0x07, 0x0a, 0x11, // parameter[9]  DATE
		0x00,                                                                   // parameter[10] TIME
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[11] DATETIME
		0x01, 0x00, // parameter[12] YEAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[13] VARCHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[14] BIT
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[15] TIMESTAMP2
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[16] DATETIME2
		0x00,                   // parameter[17] TIME2
		0x03, 0x66, 0x6f, 0x6f, // parameter[18] JSON
		0x03, 0x66, 0x6f, 0x6f, // parameter[19] DECIMAL
		0x03, 0x66, 0x6f, 0x6f, // parameter[20] ENUM
		0x03, 0x66, 0x6f, 0x6f, // parameter[21] SET
		0x03, 0x66, 0x6f, 0x6f, // parameter[22] TINY_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[23] MEDIUM_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[24] LONG_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[25] BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[26] VAR_CHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[27] CHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[28] GEOMETRY

		0x03, 0x66, 0x6f, 0x6f, // parameter[29] DECIMAL
		0x00,       // parameter[30] INT8
		0x00, 0x00, // parameter[31] INT16
		0x00, 0x00, 0x00, 0x00, // parameter[32] INT32
		0x00, 0x00, 0x00, 0x00, // parameter[33] FLOAT32
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // parameter[34] FLOAT64
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[35] TIMESTAMP
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, // parameter[36] INT64
		0x0b, 0xda, 0x07, 0x0a, // parameter[37] INT24
		0x04, 0xda, 0x07, 0x0a, 0x11, // parameter[38] DATE
		0x00,                                                                   // parameter[39] TIME
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[40] DATETIME
		0x01, 0x00, // parameter[41] YEAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[42] VARCHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[43] BIT
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[44] TIMESTAMP2
		0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00, // parameter[45] DATETIME2
		0x00,                   // parameter[46] TIME2
		0x03, 0x66, 0x6f, 0x6f, // parameter[47] JSON
		0x03, 0x66, 0x6f, 0x6f, // parameter[48] DECIMAL
		0x03, 0x66, 0x6f, 0x6f, // parameter[49] ENUM
		0x03, 0x66, 0x6f, 0x6f, // parameter[50] SET
		0x03, 0x66, 0x6f, 0x6f, // parameter[51] TINY_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[52] MEDIUM_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[53] LONG_BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[54] BLOB
		0x03, 0x66, 0x6f, 0x6f, // parameter[55] VAR_CHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[56] CHAR
		0x03, 0x66, 0x6f, 0x6f, // parameter[57] GEOMETRY
	}

	stmtID, _, err := sConn.parseComStmtExecute(cConn.PrepareData, data)
	if err != nil {
		t.Fatalf("parseComStmtExeute failed: %v", err)
	}
	if stmtID != 123 {
		t.Fatalf("Parsed incorrect values")
	}

	for i, pt := range paramTypes {
		if cConn.PrepareData[123].ParamsType[i] != int32(pt) {
			t.Fatalf("Parsed incorrect type/flag for parameter")
		}
	}
}

func TestComStmtFetch(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, _ := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare

	// This is simulated packets for `select * from test_table where id = ?`
	data := []byte{23, 18, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	stmtID, cursorType, err := sConn.parseComStmtExecute(cConn.PrepareData, data)
	if err != nil {
		t.Fatalf("parseComStmtExeute failed: %v", err)
	}
	if stmtID != 18 {
		t.Fatalf("Parsed incorrect values")
	}
	if cursorType != ReadOnly {
		t.Fatalf("Expected read-only cursor")
	}
}

func TestComStmtClose(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepare, result := MockPrepareData(t)
	cConn.PrepareData = make(map[uint32]*PrepareData)
	cConn.PrepareData[prepare.StatementID] = prepare
	if err := cConn.writePrepare(context.Background(), result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtClose, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket(context.Background())
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, ok := sConn.parseComStmtClose(data)
	if !ok {
		t.Fatalf("parseComStmtClose failed")
	}
	if stmtID != prepare.StatementID {
		t.Fatalf("Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)
	}
}

func TestQueries(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Smallest result
	checkQuery(t, "tiny", sConn, cConn, &sqltypes.Result{})

	// Typical Insert result
	checkQuery(t, "insert", sConn, cConn, &sqltypes.Result{
		RowsAffected: 0x8010203040506070,
		InsertID:     0x0102030405060708,
	})

	// Typical Select with TYPE_AND_NAME.
	// One value is also NULL.
	checkQuery(t, "type and name", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
				sqltypes.NULL,
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_AND_NAME.
	// All types are represented.
	// One row has all NULL values.
	checkQuery(t, "all types", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Type_INT8     ", Type: querypb.Type_INT8},
			{Name: "Type_UINT8    ", Type: querypb.Type_UINT8},
			{Name: "Type_INT16    ", Type: querypb.Type_INT16},
			{Name: "Type_UINT16   ", Type: querypb.Type_UINT16},
			{Name: "Type_INT24    ", Type: querypb.Type_INT24},
			{Name: "Type_UINT24   ", Type: querypb.Type_UINT24},
			{Name: "Type_INT32    ", Type: querypb.Type_INT32},
			{Name: "Type_UINT32   ", Type: querypb.Type_UINT32},
			{Name: "Type_INT64    ", Type: querypb.Type_INT64},
			{Name: "Type_UINT64   ", Type: querypb.Type_UINT64},
			{Name: "Type_FLOAT32  ", Type: querypb.Type_FLOAT32},
			{Name: "Type_FLOAT64  ", Type: querypb.Type_FLOAT64},
			{Name: "Type_TIMESTAMP", Type: querypb.Type_TIMESTAMP},
			{Name: "Type_DATE     ", Type: querypb.Type_DATE},
			{Name: "Type_TIME     ", Type: querypb.Type_TIME},
			{Name: "Type_DATETIME ", Type: querypb.Type_DATETIME},
			{Name: "Type_YEAR     ", Type: querypb.Type_YEAR},
			{Name: "Type_DECIMAL  ", Type: querypb.Type_DECIMAL},
			{Name: "Type_TEXT     ", Type: querypb.Type_TEXT},
			{Name: "Type_BLOB     ", Type: querypb.Type_BLOB},
			{Name: "Type_VARCHAR  ", Type: querypb.Type_VARCHAR},
			{Name: "Type_VARBINARY", Type: querypb.Type_VARBINARY},
			{Name: "Type_CHAR     ", Type: querypb.Type_CHAR},
			{Name: "Type_BINARY   ", Type: querypb.Type_BINARY},
			{Name: "Type_BIT      ", Type: querypb.Type_BIT},
			{Name: "Type_ENUM     ", Type: querypb.Type_ENUM},
			{Name: "Type_SET      ", Type: querypb.Type_SET},
			// Skip TUPLE, not possible in Result.
			{Name: "Type_GEOMETRY ", Type: querypb.Type_GEOMETRY},
			{Name: "Type_JSON     ", Type: querypb.Type_JSON},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT8, []byte("Type_INT8")),
				sqltypes.MakeTrusted(querypb.Type_UINT8, []byte("Type_UINT8")),
				sqltypes.MakeTrusted(querypb.Type_INT16, []byte("Type_INT16")),
				sqltypes.MakeTrusted(querypb.Type_UINT16, []byte("Type_UINT16")),
				sqltypes.MakeTrusted(querypb.Type_INT24, []byte("Type_INT24")),
				sqltypes.MakeTrusted(querypb.Type_UINT24, []byte("Type_UINT24")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("Type_INT32")),
				sqltypes.MakeTrusted(querypb.Type_UINT32, []byte("Type_UINT32")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("Type_INT64")),
				sqltypes.MakeTrusted(querypb.Type_UINT64, []byte("Type_UINT64")),
				sqltypes.MakeTrusted(querypb.Type_FLOAT32, []byte("Type_FLOAT32")),
				sqltypes.MakeTrusted(querypb.Type_FLOAT64, []byte("Type_FLOAT64")),
				sqltypes.MakeTrusted(querypb.Type_TIMESTAMP, []byte("Type_TIMESTAMP")),
				sqltypes.MakeTrusted(querypb.Type_DATE, []byte("Type_DATE")),
				sqltypes.MakeTrusted(querypb.Type_TIME, []byte("Type_TIME")),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("Type_DATETIME")),
				sqltypes.MakeTrusted(querypb.Type_YEAR, []byte("Type_YEAR")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("Type_DECIMAL")),
				sqltypes.MakeTrusted(querypb.Type_TEXT, []byte("Type_TEXT")),
				sqltypes.MakeTrusted(querypb.Type_BLOB, []byte("Type_BLOB")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("Type_VARCHAR")),
				sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte("Type_VARBINARY")),
				sqltypes.MakeTrusted(querypb.Type_CHAR, []byte("Type_CHAR")),
				sqltypes.MakeTrusted(querypb.Type_BINARY, []byte("Type_BINARY")),
				sqltypes.MakeTrusted(querypb.Type_BIT, []byte("Type_BIT")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Type_ENUM")),
				sqltypes.MakeTrusted(querypb.Type_SET, []byte("Type_SET")),
				sqltypes.MakeTrusted(querypb.Type_GEOMETRY, []byte("Type_GEOMETRY")),
				sqltypes.MakeTrusted(querypb.Type_JSON, []byte("Type_JSON")),
			},
			{
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
				sqltypes.NULL,
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_AND_NAME.
	// First value first column is an empty string, so it's encoded as 0.
	checkQuery(t, "first empty string", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with TYPE_ONLY.
	checkQuery(t, "type only", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("10")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("20")),
			},
		},
		RowsAffected: 2,
	})

	// Typical Select with ALL.
	checkQuery(t, "complete", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:         querypb.Type_INT64,
				Name:         "cool column name",
				Table:        "table name",
				OrgTable:     "org table",
				Database:     "fine db",
				OrgName:      "crazy org",
				ColumnLength: 0x80020304,
				Charset:      0x1234,
				Decimals:     36,
				Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
					querypb.MySqlFlag_PRI_KEY_FLAG |
					querypb.MySqlFlag_PART_KEY_FLAG |
					querypb.MySqlFlag_NUM_FLAG),
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("10")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("20")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("30")),
			},
		},
		RowsAffected: 3,
	})
}

func checkQuery(t *testing.T, query string, sConn, cConn *Conn, result *sqltypes.Result) {
	// The protocol depends on the CapabilityClientDeprecateEOF flag.
	// So we want to test both cases.

	sConn.Capabilities = 0
	cConn.Capabilities = 0
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */, false /* warnings */)

	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, true /* warnings */)

	sConn.Capabilities = CapabilityClientDeprecateEOF
	cConn.Capabilities = CapabilityClientDeprecateEOF
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */, false /* warnings */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */, false /* warnings */)

	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */, true /* warnings */)
}

func checkQueryInternal(t *testing.T, query string, sConn, cConn *Conn, result *sqltypes.Result, wantfields, allRows, warnings bool) {
	if sConn.Capabilities&CapabilityClientDeprecateEOF > 0 {
		query += " NOEOF"
	} else {
		query += " EOF"
	}
	if wantfields {
		query += " FIELDS"
	} else {
		query += " NOFIELDS"
	}
	if allRows {
		query += " ALL"
	} else {
		query += " PARTIAL"
	}

	var warningCount uint16
	if warnings {
		query += " WARNINGS"
		warningCount = 99
	} else {
		query += " NOWARNINGS"
	}

	var fatalError string
	// Use a go routine to run ExecuteFetch.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Test ExecuteFetch.
		maxrows := 10000
		if !allRows {
			// Asking for just one row max. The results that have more will fail.
			maxrows = 1
		}
		got, gotWarnings, err := cConn.ExecuteFetchWithWarningCount(query, maxrows, wantfields)
		if !allRows && len(result.Rows) > 1 {
			if err == nil {
				t.Errorf("ExecuteFetch should have failed but got: %v", got)
			}
			sqlErr, ok := err.(*SQLError)
			if !ok || sqlErr.Number() != ERVitessMaxRowsExceeded {
				t.Errorf("Expected ERVitessMaxRowsExceeded %v, got %v", ERVitessMaxRowsExceeded, sqlErr.Number())
			}
			return
		}
		if err != nil {
			fatalError = fmt.Sprintf("executeFetch failed: %v", err)
			return
		}
		expected := *result
		if !wantfields {
			expected.Fields = nil
		}
		if !got.Equal(&expected) {
			for i, f := range got.Fields {
				if i < len(expected.Fields) && !proto.Equal(f, expected.Fields[i]) {
					t.Logf("Got      field(%v) = %v", i, f)
					t.Logf("Expected field(%v) = %v", i, expected.Fields[i])
				}
			}
			fatalError = fmt.Sprintf("ExecuteFetch(wantfields=%v) returned:\n%v\nBut was expecting:\n%v", wantfields, got, expected)
			return
		}

		if gotWarnings != warningCount {
			t.Errorf("ExecuteFetch(%v) expected %v warnings got %v", query, warningCount, gotWarnings)
		}

		// Test ExecuteStreamFetch, build a Result.
		expected = *result
		if err := cConn.ExecuteStreamFetch(query); err != nil {
			fatalError = fmt.Sprintf("ExecuteStreamFetch(%v) failed: %v", query, err)
			return
		}
		got = &sqltypes.Result{}
		got.RowsAffected = result.RowsAffected
		got.InsertID = result.InsertID
		got.Fields, err = cConn.Fields()
		if err != nil {
			fatalError = fmt.Sprintf("Fields(%v) failed: %v", query, err)
			return
		}
		if len(got.Fields) == 0 {
			got.Fields = nil
		}
		for {
			row, err := cConn.FetchNext()
			if err != nil {
				fatalError = fmt.Sprintf("FetchNext(%v) failed: %v", query, err)
				return
			}
			if row == nil {
				// Done.
				break
			}
			got.Rows = append(got.Rows, row)
		}
		cConn.CloseResult()

		if !got.Equal(&expected) {
			for i, f := range got.Fields {
				if i < len(expected.Fields) && !proto.Equal(f, expected.Fields[i]) {
					t.Logf("========== Got      field(%v) = %v", i, f)
					t.Logf("========== Expected field(%v) = %v", i, expected.Fields[i])
				}
			}
			for i, row := range got.Rows {
				if i < len(expected.Rows) && !reflect.DeepEqual(row, expected.Rows[i]) {
					t.Logf("========== Got      row(%v) = %v", i, RowString(row))
					t.Logf("========== Expected row(%v) = %v", i, RowString(expected.Rows[i]))
				}
			}
			t.Errorf("\nExecuteStreamFetch(%v) returned:\n%+v\nBut was expecting:\n%+v\n", query, got, &expected)
		}
	}()

	// The other side gets the request, and sends the result.
	// Twice, once for ExecuteFetch, once for ExecuteStreamFetch.
	count := 2
	if !allRows && len(result.Rows) > 1 {
		// short-circuit one test, the go routine returned and didn't
		// do the streaming query.
		count--
	}

	handler := testHandler{
		result:   result,
		warnings: warningCount,
	}

	ctx := context.Background()
	for i := 0; i < count; i++ {
		err := sConn.handleNextCommand(ctx, &handler)
		if err != nil {
			t.Fatalf("error handling command: %v", err)
		}
	}

	wg.Wait()

	if fatalError != "" {
		t.Fatalf(fatalError)
	}
}

//lint:ignore U1000 for now, because deleting this function causes more errors
func writeResult(conn *Conn, result *sqltypes.Result) error {
	if len(result.Fields) == 0 {
		return conn.writeOKPacket(result.RowsAffected, result.InsertID, conn.StatusFlags, 0)
	}
	if err := conn.writeFields(result); err != nil {
		return err
	}
	if err := conn.writeRows(result); err != nil {
		return err
	}
	return conn.writeEndResult(false, 0, 0, 0)
}

func RowString(row []sqltypes.Value) string {
	l := len(row)
	result := fmt.Sprintf("%v values:", l)
	for _, val := range row {
		result += fmt.Sprintf(" %v", val)
	}
	return result
}

func writeRawPacketToConn(c *Conn, packet []byte) error {
	c.sequence = 0
	data := c.startEphemeralPacket(len(packet))
	copy(data, packet)
	return c.writeEphemeralPacket()
}

type testExec struct {
	query             string
	useCursor         byte
	expectedNumFields int
	expectedNumRows   int
	maxRows           int
}

func clientExecute(t *testing.T, cConn *Conn, useCursor byte, maxRows int) (*sqltypes.Result, serverStatus) {
	if useCursor != 0 {
		cConn.StatusFlags |= uint16(ServerCursorExists)
	} else {
		cConn.StatusFlags &= ^uint16(ServerCursorExists)
	}

	// Write a COM_STMT_EXECUTE packet
	mockPacket := []byte{ComStmtExecute, 0, 0, 0, 0, useCursor, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	if err := writeRawPacketToConn(cConn, mockPacket); err != nil {
		t.Fatalf("WriteMockExecuteToConn failed with error: %v", err)
	}

	qr, status, _, err := cConn.ReadQueryResult(context.Background(), maxRows, true)
	if err != nil {
		t.Fatalf("ReadQueryResult failed with error: %v", err)
	}

	return qr, status
}

func clientFetch(t *testing.T, cConn *Conn, useCursor byte, maxRows int, fields []*querypb.Field) (*sqltypes.Result, serverStatus) {
	// Write a COM_STMT_FETCH packet
	mockPacket := []byte{ComStmtFetch, 0, 0, 0, 0, useCursor, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	if err := writeRawPacketToConn(cConn, mockPacket); err != nil {
		t.Fatalf("WriteMockDataToConn failed with error: %v", err)
	}

	qr, status, _, err := cConn.FetchQueryResult(context.Background(), maxRows, fields)
	if err != nil && err != io.EOF {
		t.Fatalf("FetchQueryResult failed with error: %v", err)
	}

	return qr, status
}

func checkExecute(t *testing.T, sConn, cConn *Conn, test testExec) {
	// Pretend a successful COM_PREPARE was sent to server
	prepare := &PrepareData{
		StatementID: 0,
		PrepareStmt: test.query,
	}
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// use go routine to emulate client calls
	var qr *sqltypes.Result
	var status serverStatus
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		qr, status = clientExecute(t, cConn, test.useCursor, test.maxRows)
	}()

	ctx := context.Background()
	// handle a single client command
	if err := sConn.handleNextCommand(ctx, &testHandler{}); err != nil {
		t.Fatalf("handleNextComamnd failed with error: %v", err)
	}

	// wait until client receives the query result back
	wg.Wait()

	var fields = qr.Fields
	if test.expectedNumFields != len(fields) {
		t.Fatalf("Expected %d fields, Received %d", test.expectedNumFields, len(fields))
	}

	// if not using cursor, we should have results without fetching
	if test.useCursor == 0 {
		if status.cursorExists() {
			t.Fatalf("Server StatusFlag should indicate that Cursor does not exist")
		}
		if test.expectedNumRows != len(qr.Rows) {
			t.Fatalf("Expected %d rows, Received %d", test.expectedNumRows, len(qr.Rows))
		}
		return
	}

	if !status.cursorExists() {
		t.Fatalf("Server StatusFlag should indicate that Cursor exists, status flags were: %d", status)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		qr, status = clientFetch(t, cConn, test.useCursor, test.maxRows, fields)
	}()

	// handle a single client command
	if err := sConn.handleNextCommand(ctx, &testHandler{}); err != nil {
		t.Fatalf("handleNextComamnd failed with error: %v", err)
	}

	// wait until client fetches the rows
	wg.Wait()

	if status.cursorExists() {
		t.Fatalf("Server StatusFlag should not indicate a new Cursor")
	}
	if !status.cursorLastRowSent() {
		t.Fatalf("Server StatusFlag should indicate that we exhausted the cursor")
	}

	if test.expectedNumRows != len(qr.Rows) {
		t.Fatalf("Expected %d rows, Received %d", test.expectedNumRows, len(qr.Rows))
	}
}

func TestExecuteQueries(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	tests := []testExec{
		{
			query:             "empty result",
			useCursor:         0,
			expectedNumFields: 3,
			expectedNumRows:   0,
			maxRows:           100,
		},
		{
			query:             "select rows",
			useCursor:         0,
			expectedNumFields: 2,
			expectedNumRows:   2,
			maxRows:           100,
		},
		{
			query:             "large batch",
			useCursor:         0,
			expectedNumFields: 2,
			expectedNumRows:   256,
			maxRows:           1000,
		},
		{
			query:             "empty result",
			useCursor:         1,
			expectedNumFields: 3,
			expectedNumRows:   0,
			maxRows:           100,
		},
		{
			query:             "select rows",
			useCursor:         1,
			expectedNumFields: 2,
			expectedNumRows:   2,
			maxRows:           100,
		},
		{
			query:             "large batch",
			useCursor:         1,
			expectedNumFields: 2,
			expectedNumRows:   256,
			maxRows:           1000,
		},
	}

	t.Run("WithoutDeprecateEOF", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.query, func(t *testing.T) {
				checkExecute(t, sConn, cConn, test)
			})
		}
	})

	sConn.Capabilities = CapabilityClientDeprecateEOF
	cConn.Capabilities = CapabilityClientDeprecateEOF
	t.Run("WithDeprecateEOF", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.query, func(t *testing.T) {
				checkExecute(t, sConn, cConn, test)
			})
		}
	})
}

func TestComStmtPrepareWithTrailingNewLine (t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "select ?;\n"
	data := MockQueryPackets(t, sql)
	if err := cConn.writePacket(data); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}

	prepare, result := MockPrepareData(t)
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	sConn.Capabilities |= CapabilityClientMultiStatements
	handler := &testHandler{
		result: result,
	}
	err := sConn.handleNextCommand(context.Background(), handler)
	if err != nil {
		t.Fatalf("handleNextCommand failed: %v", err)
	}

	if err := cConn.ExecuteStreamFetch(sql); err != nil {
		t.Fatalf("ExecuteStreamFetch(%v) failed: %v", sql, err)
		return
	}
}

func TestComStmtPrepareMultiStmt (t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "select ?; select ?;"
	data := MockQueryPackets(t, sql)
	if err := cConn.writePacket(data); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}

	prepare, result := MockPrepareData(t)
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	sConn.Capabilities |= CapabilityClientMultiStatements
	handler := &testHandler{
		result: result,
	}
	err := sConn.handleNextCommand(context.Background(), handler)
	if err != nil {
		t.Fatalf("handleNextCommand failed: %v", err)
	}

	if err := cConn.ExecuteStreamFetch(sql); err == nil {
		t.Fatalf("expected error, but received nil")
		return
	}
}
