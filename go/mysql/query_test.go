/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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

	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

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
	if db != "my_db" {
		t.Errorf("parseComInitDB returned unexpected data: %v", db)
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

	// Typicall Select with TYPE_AND_NAME.
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

	// Typicall Select with TYPE_AND_NAME.
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

	// Typicall Select with TYPE_AND_NAME.
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

	// Typicall Select with TYPE_ONLY.
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

	// Typicall Select with ALL.
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
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */)

	sConn.Capabilities = CapabilityClientDeprecateEOF
	cConn.Capabilities = CapabilityClientDeprecateEOF
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, true /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, true /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, true /* wantfields */, false /* allRows */)
	checkQueryInternal(t, query, sConn, cConn, result, false /* wantfields */, false /* allRows */)
}

func checkQueryInternal(t *testing.T, query string, sConn, cConn *Conn, result *sqltypes.Result, wantfields, allRows bool) {

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
		got, err := cConn.ExecuteFetch(query, maxrows, wantfields)
		if !allRows && len(result.Rows) > 1 {
			if err == nil {
				t.Errorf("ExecuteFetch should have failed but got: %v", got)
			}
			return
		}
		if err != nil {
			t.Fatalf("executeFetch failed: %v", err)
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
			t.Fatalf("ExecuteFetch(wantfields=%v) returned:\n%v\nBut was expecting:\n%v", wantfields, got, expected)
		}

		// Test ExecuteStreamFetch, build a Result.
		expected = *result
		if err := cConn.ExecuteStreamFetch(query); err != nil {
			t.Fatalf("ExecuteStreamFetch(%v) failed: %v", query, err)
		}
		got = &sqltypes.Result{}
		got.RowsAffected = result.RowsAffected
		got.InsertID = result.InsertID
		got.Fields, err = cConn.Fields()
		if err != nil {
			t.Fatalf("Fields(%v) failed: %v", query, err)
		}
		if len(got.Fields) == 0 {
			got.Fields = nil
		}
		for {
			row, err := cConn.FetchNext()
			if err != nil {
				t.Fatalf("FetchNext(%v) failed: %v", query, err)
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

	for i := 0; i < count; i++ {
		comQuery, err := sConn.ReadPacket()
		if err != nil {
			t.Fatalf("server cannot read query: %v", err)
		}
		if comQuery[0] != ComQuery {
			t.Fatalf("server got bad packet: %v", comQuery)
		}
		got := string(sConn.parseComQuery(comQuery))
		if got != query {
			t.Errorf("server got query '%v' but expected '%v'", got, query)
		}
		if err := writeResult(sConn, result); err != nil {
			t.Errorf("Error writing result to client: %v", err)
		}
		sConn.sequence = 0
	}

	wg.Wait()
}

func writeResult(conn *Conn, result *sqltypes.Result) error {
	if err := conn.writeFields(result); err != nil {
		return err
	}
	if len(result.Fields) == 0 {
		return nil
	}
	if err := conn.writeRows(result); err != nil {
		return err
	}
	return conn.writeEndResult()
}

func RowString(row []sqltypes.Value) string {
	l := len(row)
	result := fmt.Sprintf("%v values:", l)
	for _, val := range row {
		result += fmt.Sprintf(" %v", val)
	}
	return result
}
