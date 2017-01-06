package mysqlconn

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttest"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// assertSQLError makes sure we get the right error.
func assertSQLError(t *testing.T, err error, code int, sqlState string, subtext string) {
	if err == nil {
		t.Fatalf("was expecting SQLError %v / %v / %v but got no error.", code, sqlState, subtext)
	}
	serr, ok := err.(*sqldb.SQLError)
	if !ok {
		t.Fatalf("was expecting SQLError %v / %v / %v but got: %v", code, sqlState, subtext, err)
	}
	if serr.Num != code {
		t.Fatalf("was expecting SQLError %v / %v / %v but got code %v", code, sqlState, subtext, serr.Num)
	}
	if serr.State != sqlState {
		t.Fatalf("was expecting SQLError %v / %v / %v but got state %v", code, sqlState, subtext, serr.State)
	}
	if subtext != "" && !strings.Contains(serr.Message, subtext) {
		t.Fatalf("was expecting SQLError %v / %v / %v but got message %v", code, sqlState, subtext, serr.Message)

	}
}

// TestConnectTimeout runs connection failure scenarios against a
// server that's not listening or has trouble.  This test is not meant
// to use a valid server. So we do not test bad handshakes here.
func TestConnectTimeout(t *testing.T) {
	// Create a socket, but it's not accepting. So all Dial
	// attempts will timeout.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port
	params := &sqldb.ConnParams{
		Host: host,
		Port: port,
	}
	defer listener.Close()

	// Test that canceling the context really interrupts the Connect.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, err := Connect(ctx, params)
		if err != context.Canceled {
			t.Errorf("Was expecting context.Canceled but got: %v", err)
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Tests a connection timeout works.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = Connect(ctx, params)
	cancel()
	if err != context.DeadlineExceeded {
		t.Errorf("Was expecting context.DeadlineExceeded but got: %v", err)
	}

	// Now the server will listen, but close all connections on accept.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Listener was closed.
				return
			}
			conn.Close()
		}
	}()
	ctx = context.Background()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRConnHostError, SSSignalException, "initial packet read failed")

	// Tests a connection where Dial fails properly returns the
	// right error. To simulate exactly the right failure, try to dial
	// a Unix socket that's just a temp file.
	fd, err := ioutil.TempFile("", "mysqlconn")
	if err != nil {
		t.Fatalf("cannot create TemFile: %v", err)
	}
	name := fd.Name()
	fd.Close()
	params.UnixSocket = name
	ctx = context.Background()
	_, err = Connect(ctx, params)
	os.Remove(name)
	assertSQLError(t, err, CRConnectionError, SSSignalException, "connection refused")
}

// TestWithRealDatabase runs a real MySQL database, and runs all kinds
// of tests on it. To minimize overhead, we only run one database, and
// run all the tests on it.
func TestWithRealDatabase(t *testing.T) {
	hdl, err := vttest.LaunchVitess(
		vttest.MySQLOnly("vttest"),
		vttest.Schema("create table a(id int, name varchar(128), primary key(id))"))
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = hdl.TearDown()
		if err != nil {
			t.Error(err)
			return
		}
	}()
	params, err := hdl.MySQLConnParams()
	if err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	conn, err := Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}

	// Try a simple error case.
	_, err = conn.ExecuteFetch("select * from aa", 1000, true)
	if err == nil || !strings.Contains(err.Error(), "Table 'vttest.aa' doesn't exist") {
		t.Fatalf("expected error but got: %v", err)
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
				Charset:      63,    // binary
				Flags:        16387, // NOT_NULL_FLAG, PRI_KEY_FLAG, PART_KEY_FLAG
			},
			{
				Name:         "name",
				Type:         querypb.Type_VARCHAR,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "name",
				ColumnLength: 384,
				Charset:      33, // utf8
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
		t.Errorf("unexpected result for select, got:\n%v\nexpected:\n%v\n", result, expectedResult)
	}

	// Now be serious: insert a thousand rows.
	timeInserts(t, &params, 1000)

	// And use a streaming query to read them back.
	// Do it twice to make sure state is reset properly.
	readRowsUsingStream(t, &params, 1001)
	readRowsUsingStream(t, &params, 1001)

	if true {
		// Return early, rest is more load tests.
		return
	}

	// More serious, even more. Get 1001 rows, 10000 times.
	timeSelects(t, &params, 10000, 1001)

	// Use the new client, do parallel reads.
	timeParallelReads(t, &params, 10, 10000)

	// Use the old client, do the same parallel query.
	timeOldParallelReads(t, params, 10, 10000)
}

func readRowsUsingStream(t *testing.T, params *sqldb.ConnParams, expectedCount int) {
	t.Logf("============= readRowsUsingStream %v expected rows", expectedCount)

	// Connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Start the streaming query.
	start := time.Now()
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
			Charset:      63,    // binary
			Flags:        16387, // NOT_NULL_FLAG, PRI_KEY_FLAG, PART_KEY_FLAG
		},
		{
			Name:         "name",
			Type:         querypb.Type_VARCHAR,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "name",
			ColumnLength: 384,
			Charset:      33, // utf8
		},
	}
	fields, err := conn.Fields()
	if err != nil {
		t.Fatalf("Fields failed: %v", err)
	}
	if !reflect.DeepEqual(fields, expectedFields) {
		t.Fatalf("fields are not right, got:\n%v\nexpected:\n%v", fields, expectedFields)
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
	t.Logf("     --> %v\n", time.Since(start))
}

func timeInserts(t *testing.T, params *sqldb.ConnParams, count int) {
	t.Logf("============= timeInserts %v rows", count)

	// Connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Time the insert.
	start := time.Now()
	for i := 0; i < count; i++ {
		_, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		if err != nil {
			t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
	}
	t.Logf("     --> %v\n", time.Since(start))
}

func timeSelects(t *testing.T, params *sqldb.ConnParams, count, expectedCount int) {
	t.Logf("============= timeSelects running %v times expecting %v rows", count, expectedCount)

	// Connect.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Time the insert.
	start := time.Now()
	for i := 0; i < count; i++ {
		result, err := conn.ExecuteFetch("select * from a", expectedCount, true)
		if err != nil {
			t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
		if len(result.Rows) != expectedCount {
			t.Fatalf("ExecuteFetch(%v) returned a weird result: %v", i, result)
		}
	}
	t.Logf("     --> %v\n", time.Since(start))
}

func timeParallelReads(t *testing.T, params *sqldb.ConnParams, parallelCount, queryCount int) {
	t.Logf("============= timeParallelReads %v threads, each running %v queries", parallelCount, queryCount)

	ctx := context.Background()
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := Connect(ctx, params)
			if err != nil {
				t.Fatal(err)
			}

			for j := 0; j < queryCount; j++ {
				result, err := conn.ExecuteFetch("select * from a", 10000, true)
				if err != nil {
					t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
				}
				if len(result.Rows) != 1001 {
					t.Fatalf("ExecuteFetch(%v) returned a weird result: %v", i, result)
				}
			}
			conn.Close()
		}()
	}
	wg.Wait()
	t.Logf("     --> %v\n", time.Since(start))
}

func timeOldParallelReads(t *testing.T, params sqldb.ConnParams, parallelCount, queryCount int) {
	t.Logf("============= timeOldParallelReads %v threads, each running %v queries", parallelCount, queryCount)

	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < parallelCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := mysql.Connect(params)
			if err != nil {
				t.Fatal(err)
			}

			for j := 0; j < queryCount; j++ {
				result, err := conn.ExecuteFetch("select * from a", 10000, true)
				if err != nil {
					t.Fatalf("ExecuteFetch(%v) failed: %v", j, err)
				}
				if len(result.Rows) != 1001 {
					t.Fatalf("ExecuteFetch(%v) returned a weird result: %v", j, result)
				}
			}

			conn.Close()
		}()
	}
	wg.Wait()
	t.Logf("     --> %v\n", time.Since(start))
}
