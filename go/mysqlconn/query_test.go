package mysqlconn

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
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
	checkQuery(t, sConn, cConn, &sqltypes.Result{})

	// Typical Insert result
	checkQuery(t, sConn, cConn, &sqltypes.Result{
		RowsAffected: 0x8010203040506070,
		InsertID:     0x0102030405060708,
	})

	// Typicall Select with TYPE_AND_NAME.
	// One value is also NULL.
	checkQuery(t, sConn, cConn, &sqltypes.Result{
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
	})

	// Typicall Select with TYPE_AND_NAME.
	// First value first column is an empty string, so it's encoded as 0.
	checkQuery(t, sConn, cConn, &sqltypes.Result{
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
	})

	// Typicall Select with TYPE_ONLY.
	checkQuery(t, sConn, cConn, &sqltypes.Result{
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
	})

	// Typicall Select with ALL.
	checkQuery(t, sConn, cConn, &sqltypes.Result{
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
				Flags:        16387, // NOT_NULL_FLAG, PRI_KEY_FLAG, PART_KEY_FLAG
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

func checkQuery(t *testing.T, sConn, cConn *Conn, result *sqltypes.Result) {
	// The protocol depends on the CapabilityClientDeprecateEOF flag.
	// So we want to test both cases.

	sConn.Capabilities = 0
	cConn.Capabilities = 0
	checkQueryInternal(t, sConn, cConn, result, true /* wantfields */, true /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, false /* wantfields */, true /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, true /* wantfields */, false /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, false /* wantfields */, false /* allRows */)

	sConn.Capabilities = CapabilityClientDeprecateEOF
	cConn.Capabilities = CapabilityClientDeprecateEOF
	checkQueryInternal(t, sConn, cConn, result, true /* wantfields */, true /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, false /* wantfields */, true /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, true /* wantfields */, false /* allRows */)
	checkQueryInternal(t, sConn, cConn, result, false /* wantfields */, false /* allRows */)
}

func checkQueryInternal(t *testing.T, sConn, cConn *Conn, result *sqltypes.Result, wantfields, allRows bool) {

	query := "query test"

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
		if !reflect.DeepEqual(got, &expected) {
			t.Fatalf("executeFetch(wantfields=%v) returned:\n%v\nBut was expecting:\n%v", wantfields, got, expected)
		}
	}()

	// The other side gets the request, and sends the result.
	comQuery, err := sConn.ReadPacket()
	if err != nil {
		t.Fatalf("server cannot read query: %v", err)
	}
	if comQuery[0] != ComQuery {
		t.Fatalf("server got bad packet: %v", comQuery)
	}
	got := sConn.parseComQuery(comQuery)
	if got != query {
		t.Errorf("server got query '%v' but expected '%v'", got, query)
	}
	if err := sConn.writeResult(result); err != nil {
		t.Errorf("Error writing result to client: %v", err)
	}
	sConn.sequence = 0

	wg.Wait()
}

func testQueriesWithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	// Try a simple error case.
	_, err = conn.ExecuteFetch("select * from aa", 1000, true)
	if err == nil || !strings.Contains(err.Error(), "Table 'vttest.aa' doesn't exist") {
		t.Fatalf("expected error but got: %v", err)
	}

	// Try a simple DDL.
	if _, err := conn.ExecuteFetch("create table a(id int, name varchar(128), primary key(id))", 0, false); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	// Try a simple insert.
	result, err := conn.ExecuteFetch("insert into a(id, name) values(10, 'nice name')", 1000, true)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}

	// And re-read what we inserted.
	result, err = conn.ExecuteFetch("select * from a", 1000, true)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         querypb.Type_INT32,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      CharacterSetBinary,
				Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
					querypb.MySqlFlag_PRI_KEY_FLAG |
					querypb.MySqlFlag_PART_KEY_FLAG),
			},
			{
				Name:         "name",
				Type:         querypb.Type_VARCHAR,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "name",
				ColumnLength: 384,
				Charset:      CharacterSetUtf8,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
		},
	}
	if !reflect.DeepEqual(result, expectedResult) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedResult.Fields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		if !reflect.DeepEqual(result, expectedResult) {
			t.Errorf("unexpected result for select, got:\n%v\nexpected:\n%v\n", result, expectedResult)
		}
	}

	// Insert a few rows.
	for i := 0; i < 100; i++ {
		_, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		if err != nil {
			t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
	}

	// And use a streaming query to read them back.
	// Do it twice to make sure state is reset properly.
	readRowsUsingStream(t, conn, 101)
	readRowsUsingStream(t, conn, 101)

	// And drop the table.
	if _, err := conn.ExecuteFetch("drop table a", 0, false); err != nil {
		t.Fatalf("drop table failed: %v", err)
	}
}

func readRowsUsingStream(t *testing.T, conn *Conn, expectedCount int) {
	// Start the streaming query.
	if err := conn.ExecuteStreamFetch("select * from a"); err != nil {
		t.Fatalf("ExecuteStreamFetch failed: %v", err)
	}

	// Check the fields.
	expectedFields := []*querypb.Field{
		{
			Name:         "id",
			Type:         querypb.Type_INT32,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "id",
			ColumnLength: 11,
			Charset:      CharacterSetBinary,
			Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
				querypb.MySqlFlag_PRI_KEY_FLAG |
				querypb.MySqlFlag_PART_KEY_FLAG),
		},
		{
			Name:         "name",
			Type:         querypb.Type_VARCHAR,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "name",
			ColumnLength: 384,
			Charset:      CharacterSetUtf8,
		},
	}
	fields, err := conn.Fields()
	if err != nil {
		t.Fatalf("Fields failed: %v", err)
	}
	if !reflect.DeepEqual(fields, expectedFields) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedFields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		if !reflect.DeepEqual(fields, expectedFields) {
			t.Fatalf("fields are not right, got:\n%v\nexpected:\n%v", fields, expectedFields)
		}
	}

	// Read the rows.
	count := 0
	for {
		row, err := conn.FetchNext()
		if err != nil {
			t.Fatalf("FetchNext failed: %v", err)
		}
		if row == nil {
			// We're done.
			break
		}
		if len(row) != 2 {
			t.Fatalf("Unexpected row found: %v", row)
		}
		count++
	}
	if count != expectedCount {
		t.Errorf("Got unexpected count %v for query, was expecting %v", count, expectedCount)
	}
	conn.CloseResult()
}
