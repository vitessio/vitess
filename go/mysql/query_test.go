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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/mysql/collations"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Utility function to write sql query as packets to test parseComPrepare
func preparePacket(t *testing.T, query string) []byte {
	data := make([]byte, len(query)+1+packetHeaderSize)
	// Not sure if it makes a difference
	pos := packetHeaderSize
	pos = writeByte(data, pos, ComPrepare)
	copy(data[pos:], query)
	return data
}

func MockPrepareData(t *testing.T) (*PrepareData, *sqltypes.Result) {
	sql := "select * from test_table where id = ?"

	result := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:    "id",
				Type:    querypb.Type_INT32,
				Charset: collations.CollationBinaryID,
				Flags:   uint32(querypb.MySqlFlag_NUM_FLAG),
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
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ComInitDB {
		t.Fatalf("sConn.ReadPacket - ComInitDB failed: %v %v", data, err)
	}
	db := sConn.parseComInitDB(data)
	assert.Equal(t, "my_db", db, "parseComInitDB returned unexpected data: %v", db)

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
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ComSetOption {
		t.Fatalf("sConn.ReadPacket - ComSetOption failed: %v %v", data, err)
	}
	operation, ok := sConn.parseComSetOption(data)
	require.True(t, ok, "parseComSetOption failed unexpectedly")
	assert.Equal(t, uint16(1), operation, "parseComSetOption returned unexpected data: %v", operation)

}

func TestComStmtPrepare(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "select * from test_table where id = ?"
	mockData := preparePacket(t, sql)

	if err := cConn.writePacket(mockData); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComPrepare failed: %v", err)

	parsedQuery := sConn.parseComPrepare(data)
	require.Equal(t, sql, parsedQuery, "Received incorrect query, want: %v, got: %v", sql, parsedQuery)

	prepare, result := MockPrepareData(t)
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// write the response to the client
	if err := sConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("sConn.writePrepare failed: %v", err)
	}

	resp, err := cConn.ReadPacket()
	require.NoError(t, err, "cConn.ReadPacket failed: %v", err)
	require.Equal(t, prepare.StatementID, uint32(resp[1]), "Received incorrect Statement ID, want: %v, got: %v", prepare.StatementID, resp[1])

}

func TestComStmtPrepareUpdStmt(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	sql := "UPDATE test SET __bit = ?, __tinyInt = ?, __tinyIntU = ?, __smallInt = ?, __smallIntU = ?, __mediumInt = ?, __mediumIntU = ?, __int = ?, __intU = ?, __bigInt = ?, __bigIntU = ?, __decimal = ?, __float = ?, __double = ?, __date = ?, __datetime = ?, __timestamp = ?, __time = ?, __year = ?, __char = ?, __varchar = ?, __binary = ?, __varbinary = ?, __tinyblob = ?, __tinytext = ?, __blob = ?, __text = ?, __enum = ?, __set = ? WHERE __id = 0"
	mockData := preparePacket(t, sql)

	err := cConn.writePacket(mockData)
	require.NoError(t, err, "writePacket failed")

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComPrepare failed")

	parsedQuery := sConn.parseComPrepare(data)
	require.Equal(t, sql, parsedQuery, "Received incorrect query")

	paramsCount := uint16(29)
	prepare := &PrepareData{
		StatementID: 1,
		PrepareStmt: sql,
		ParamsCount: paramsCount,
	}
	sConn.PrepareData = make(map[uint32]*PrepareData)
	sConn.PrepareData[prepare.StatementID] = prepare

	// write the response to the client
	err = sConn.writePrepare(nil, prepare)
	require.NoError(t, err, "sConn.writePrepare failed")

	resp, err := cConn.ReadPacket()
	require.NoError(t, err, "cConn.ReadPacket failed")
	require.EqualValues(t, prepare.StatementID, resp[1], "Received incorrect Statement ID")

	for i := uint16(0); i < paramsCount; i++ {
		resp, err := cConn.ReadPacket()
		require.NoError(t, err, "cConn.ReadPacket failed")
		require.EqualValues(t, 0xfd, resp[17], "Received incorrect Statement ID")
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
	if err := cConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtSendLongData, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, paramID, chunkData, ok := sConn.parseComStmtSendLongData(data)
	require.True(t, ok, "parseComStmtSendLongData failed")
	require.Equal(t, uint16(1), paramID, "Received incorrect ParamID, want %v, got %v:", paramID, 1)
	require.Equal(t, prepare.StatementID, stmtID, "Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)
	// Check length of chunkData, Since its a subset of `data` and compare with it after we subtract the number of bytes that was read from it.
	// sizeof(uint32) + sizeof(uint16) + 1 = 7
	require.Equal(t, len(data)-7, len(chunkData), "Received bad chunkData")

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
	data := []byte{23, 18, 0, 0, 0, 128, 1, 0, 0, 0, 0, 1, 1, 128, 1}

	stmtID, _, err := sConn.parseComStmtExecute(cConn.PrepareData, data)
	require.NoError(t, err, "parseComStmtExeute failed: %v", err)
	require.Equal(t, uint32(18), stmtID, "Parsed incorrect values")

}

func TestComStmtExecuteUpdStmt(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	prepareDataMap := map[uint32]*PrepareData{
		1: {
			StatementID: 1,
			ParamsCount: 29,
			ParamsType:  make([]int32, 29),
			BindVars:    map[string]*querypb.BindVariable{},
		}}

	// This is simulated packets for update query
	data := []byte{
		0x29, 0x01, 0x00, 0x00, 0x17, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x01, 0x10, 0x00, 0x01, 0x00, 0x01, 0x80, 0x02, 0x00, 0x02, 0x80, 0x03, 0x00, 0x03,
		0x80, 0x03, 0x00, 0x03, 0x80, 0x08, 0x00, 0x08, 0x80, 0x00, 0x00, 0x04, 0x00, 0x05, 0x00, 0x0a,
		0x00, 0x0c, 0x00, 0x07, 0x00, 0x0b, 0x00, 0x0d, 0x80, 0xfe, 0x00, 0xfe, 0x00, 0xfc, 0x00, 0xfc,
		0x00, 0xfc, 0x00, 0xfe, 0x00, 0xfc, 0x00, 0xfe, 0x00, 0xfe, 0x00, 0xfe, 0x00, 0x08, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0xaa, 0xe0, 0x80, 0xff, 0x00, 0x80, 0xff, 0xff, 0x00, 0x00, 0x80, 0xff,
		0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x15, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x2e, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 0xd0, 0x0f, 0x49, 0x40, 0x44, 0x17, 0x41, 0x54, 0xfb, 0x21, 0x09, 0x40, 0x04, 0xe0,
		0x07, 0x08, 0x08, 0x0b, 0xe0, 0x07, 0x08, 0x08, 0x11, 0x19, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x0b,
		0xe0, 0x07, 0x08, 0x08, 0x11, 0x19, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x01, 0x08, 0x00, 0x00,
		0x00, 0x07, 0x3b, 0x3b, 0x00, 0x00, 0x00, 0x00, 0x04, 0x31, 0x39, 0x39, 0x39, 0x08, 0x31, 0x32,
		0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9, 0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f,
		0xe8, 0xb5, 0x9e, 0x08, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x08, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37, 0x38, 0x08, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9,
		0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f, 0xe8, 0xb5, 0x9e, 0x08, 0x31, 0x32, 0x33, 0x34,
		0x35, 0x36, 0x37, 0x38, 0x0c, 0xe9, 0x9f, 0xa9, 0xe5, 0x86, 0xac, 0xe7, 0x9c, 0x9f, 0xe8, 0xb5,
		0x9e, 0x03, 0x66, 0x6f, 0x6f, 0x07, 0x66, 0x6f, 0x6f, 0x2c, 0x62, 0x61, 0x72}

	stmtID, _, err := sConn.parseComStmtExecute(prepareDataMap, data[4:]) // first 4 are header
	require.NoError(t, err)
	require.EqualValues(t, 1, stmtID)

	prepData := prepareDataMap[stmtID]
	assert.EqualValues(t, querypb.Type_BIT, prepData.ParamsType[0], "got: %s", querypb.Type(prepData.ParamsType[0]))
	assert.EqualValues(t, querypb.Type_INT8, prepData.ParamsType[1], "got: %s", querypb.Type(prepData.ParamsType[1]))
	assert.EqualValues(t, querypb.Type_INT8, prepData.ParamsType[2], "got: %s", querypb.Type(prepData.ParamsType[2]))
	assert.EqualValues(t, querypb.Type_INT16, prepData.ParamsType[3], "got: %s", querypb.Type(prepData.ParamsType[3]))
	assert.EqualValues(t, querypb.Type_INT16, prepData.ParamsType[4], "got: %s", querypb.Type(prepData.ParamsType[4]))
	assert.EqualValues(t, querypb.Type_INT32, prepData.ParamsType[5], "got: %s", querypb.Type(prepData.ParamsType[5]))
	assert.EqualValues(t, querypb.Type_INT32, prepData.ParamsType[6], "got: %s", querypb.Type(prepData.ParamsType[6]))
	assert.EqualValues(t, querypb.Type_INT32, prepData.ParamsType[7], "got: %s", querypb.Type(prepData.ParamsType[7]))
	assert.EqualValues(t, querypb.Type_INT32, prepData.ParamsType[8], "got: %s", querypb.Type(prepData.ParamsType[8]))
	assert.EqualValues(t, querypb.Type_INT64, prepData.ParamsType[9], "got: %s", querypb.Type(prepData.ParamsType[9]))
	assert.EqualValues(t, querypb.Type_INT64, prepData.ParamsType[10], "got: %s", querypb.Type(prepData.ParamsType[10]))
	assert.EqualValues(t, querypb.Type_DECIMAL, prepData.ParamsType[11], "got: %s", querypb.Type(prepData.ParamsType[11]))
	assert.EqualValues(t, querypb.Type_FLOAT32, prepData.ParamsType[12], "got: %s", querypb.Type(prepData.ParamsType[12]))
	assert.EqualValues(t, querypb.Type_FLOAT64, prepData.ParamsType[13], "got: %s", querypb.Type(prepData.ParamsType[13]))
	assert.EqualValues(t, querypb.Type_DATE, prepData.ParamsType[14], "got: %s", querypb.Type(prepData.ParamsType[14]))
	assert.EqualValues(t, querypb.Type_DATETIME, prepData.ParamsType[15], "got: %s", querypb.Type(prepData.ParamsType[15]))
	assert.EqualValues(t, querypb.Type_TIMESTAMP, prepData.ParamsType[16], "got: %s", querypb.Type(prepData.ParamsType[16]))
	assert.EqualValues(t, querypb.Type_TIME, prepData.ParamsType[17], "got: %s", querypb.Type(prepData.ParamsType[17]))

	// this is year but in binary it is changed to varbinary
	assert.EqualValues(t, querypb.Type_VARBINARY, prepData.ParamsType[18], "got: %s", querypb.Type(prepData.ParamsType[18]))

	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[19], "got: %s", querypb.Type(prepData.ParamsType[19]))
	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[20], "got: %s", querypb.Type(prepData.ParamsType[20]))
	assert.EqualValues(t, querypb.Type_TEXT, prepData.ParamsType[21], "got: %s", querypb.Type(prepData.ParamsType[21]))
	assert.EqualValues(t, querypb.Type_TEXT, prepData.ParamsType[22], "got: %s", querypb.Type(prepData.ParamsType[22]))
	assert.EqualValues(t, querypb.Type_TEXT, prepData.ParamsType[23], "got: %s", querypb.Type(prepData.ParamsType[23]))
	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[24], "got: %s", querypb.Type(prepData.ParamsType[24]))
	assert.EqualValues(t, querypb.Type_TEXT, prepData.ParamsType[25], "got: %s", querypb.Type(prepData.ParamsType[25]))
	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[26], "got: %s", querypb.Type(prepData.ParamsType[26]))
	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[27], "got: %s", querypb.Type(prepData.ParamsType[27]))
	assert.EqualValues(t, querypb.Type_CHAR, prepData.ParamsType[28], "got: %s", querypb.Type(prepData.ParamsType[28]))
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
	if err := cConn.writePrepare(result.Fields, prepare); err != nil {
		t.Fatalf("writePrepare failed: %v", err)
	}

	// Since there's no writeComStmtClose, we'll write a prepareStmt and check if we can read the StatementID
	data, err := sConn.ReadPacket()
	if err != nil || len(data) == 0 {
		t.Fatalf("sConn.ReadPacket - ComStmtClose failed: %v %v", data, err)
	}
	stmtID, ok := sConn.parseComStmtClose(data)
	require.True(t, ok, "parseComStmtClose failed")
	require.Equal(t, prepare.StatementID, stmtID, "Received incorrect value, want: %v, got: %v", uint32(data[1]), prepare.StatementID)

}

// This test has been added to verify that IO errors in a connection lead to SQL Server lost errors
// So that we end up closing the connection higher up the stack and not reusing it.
// This test was added in response to a panic that was run into.
func TestSQLErrorOnServerClose(t *testing.T) {
	// Create socket pair for the server and client
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.WriteComQuery("close before rows read")
	require.NoError(t, err)

	handler := &testRun{t: t}
	_ = sConn.handleNextCommand(handler)

	// From the server we will receive a field packet which the client will read
	// At that point, if the server crashes and closes the connection.
	// We should be getting a Connection lost error.
	_, _, _, err = cConn.ReadQueryResult(100, true)
	require.Error(t, err)
	require.True(t, sqlerror.IsConnLostDuringQuery(err), err.Error())
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
				Name:    "id",
				Type:    querypb.Type_INT32,
				Charset: collations.CollationBinaryID,
				Flags:   uint32(querypb.MySqlFlag_NUM_FLAG),
			},
			{
				Name:    "name",
				Type:    querypb.Type_VARCHAR,
				Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
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
	})

	// Typical Select with TYPE_AND_NAME.
	// All types are represented.
	// One row has all NULL values.
	checkQuery(t, "all types", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "Type_INT8     ", Type: querypb.Type_INT8, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_UINT8    ", Type: querypb.Type_UINT8, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Type_INT16    ", Type: querypb.Type_INT16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_UINT16   ", Type: querypb.Type_UINT16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Type_INT24    ", Type: querypb.Type_INT24, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_UINT24   ", Type: querypb.Type_UINT24, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Type_INT32    ", Type: querypb.Type_INT32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_UINT32   ", Type: querypb.Type_UINT32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Type_INT64    ", Type: querypb.Type_INT64, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_UINT64   ", Type: querypb.Type_UINT64, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Type_FLOAT32  ", Type: querypb.Type_FLOAT32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_FLOAT64  ", Type: querypb.Type_FLOAT64, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_TIMESTAMP", Type: querypb.Type_TIMESTAMP, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_TIMESTAMP_FLAG)},
			{Name: "Type_DATE     ", Type: querypb.Type_DATE, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_TIME     ", Type: querypb.Type_TIME, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_DATETIME ", Type: querypb.Type_DATETIME, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_YEAR     ", Type: querypb.Type_YEAR, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_DECIMAL  ", Type: querypb.Type_DECIMAL, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
			{Name: "Type_TEXT     ", Type: querypb.Type_TEXT, Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
			{Name: "Type_BLOB     ", Type: querypb.Type_BLOB, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_VARCHAR  ", Type: querypb.Type_VARCHAR, Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
			{Name: "Type_VARBINARY", Type: querypb.Type_VARBINARY, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_CHAR     ", Type: querypb.Type_CHAR, Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
			{Name: "Type_BINARY   ", Type: querypb.Type_BINARY, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_BIT      ", Type: querypb.Type_BIT, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			{Name: "Type_ENUM     ", Type: querypb.Type_ENUM, Charset: uint32(collations.MySQL8().DefaultConnectionCharset()), Flags: uint32(querypb.MySqlFlag_ENUM_FLAG)},
			{Name: "Type_SET      ", Type: querypb.Type_SET, Charset: uint32(collations.MySQL8().DefaultConnectionCharset()), Flags: uint32(querypb.MySqlFlag_SET_FLAG)},
			// Skip TUPLE, not possible in Result.
			{Name: "Type_GEOMETRY ", Type: querypb.Type_GEOMETRY, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG | querypb.MySqlFlag_BLOB_FLAG)},
			{Name: "Type_JSON     ", Type: querypb.Type_JSON, Charset: collations.CollationUtf8mb4ID},
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
	})

	// Typical Select with TYPE_AND_NAME.
	// First value first column is an empty string, so it's encoded as 0.
	checkQuery(t, "first empty string", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:    "name",
				Type:    querypb.Type_VARCHAR,
				Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
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
	})

	// Typical Select with TYPE_ONLY.
	checkQuery(t, "type only", sConn, cConn, &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Type:    querypb.Type_INT64,
				Charset: collations.CollationBinaryID,
				Flags:   uint32(querypb.MySqlFlag_NUM_FLAG),
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

		maxrows := 10000
		if !allRows {
			// Asking for just one row max. The results that have more will fail.
			maxrows = 1
		}
		got, gotWarnings, err := cConn.ExecuteFetchWithWarningCount(query, maxrows, wantfields)
		if !allRows && len(result.Rows) > 1 {
			require.ErrorContains(t, err, "Row count exceeded")
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
					t.Logf("Query = %v", query)
					t.Logf("Got      field(%v) = %v", i, f)
					t.Logf("Expected field(%v) = %v", i, expected.Fields[i])
				}
			}
			fatalError = fmt.Sprintf("ExecuteFetch(wantfields=%v) returned:\n%v\nBut was expecting:\n%v", wantfields, got, expected)
			return
		}

		if gotWarnings != warningCount {
			t.Errorf("ExecuteFetch(%v) expected %v warnings got %v", query, warningCount, gotWarnings)
			return
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
			row, err := cConn.FetchNext(nil)
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
			if expected.RowsAffected != got.RowsAffected {
				t.Logf("========== Got      RowsAffected = %v", got.RowsAffected)
				t.Logf("========== Expected RowsAffected = %v", expected.RowsAffected)
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

	for i := 0; i < count; i++ {
		kontinue := sConn.handleNextCommand(&handler)
		require.True(t, kontinue, "error handling command: %d", i)

	}

	wg.Wait()
	require.Equal(t, "", fatalError, fatalError)

}

func RowString(row []sqltypes.Value) string {
	l := len(row)
	result := fmt.Sprintf("%v values:", l)
	for _, val := range row {
		result += fmt.Sprintf(" %v", val)
	}
	return result
}
