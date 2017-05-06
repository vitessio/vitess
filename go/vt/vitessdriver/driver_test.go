/*
Copyright 2017 Google Inc.

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

package vitessdriver

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/vtgate/grpcvtgateservice"
)

var (
	testAddress string
)

// TestMain tests the Vitess Go SQL driver.
//
// Note that the queries used in the test are not valid SQL queries and don't
// have to be. The main point here is to test the interactions against a
// vtgate implementation (here: fakeVTGateService from fakeserver_test.go).
func TestMain(m *testing.M) {
	service := CreateFakeServer()

	// listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("Cannot listen: %v", err))
	}

	// Create a gRPC server and listen on the port.
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	testAddress = listener.Addr().String()
	os.Exit(m.Run())
}

func TestOpen(t *testing.T) {
	var testcases = []struct {
		desc    string
		connStr string
		conn    *conn
	}{
		{
			desc:    "Open()",
			connStr: fmt.Sprintf(`{"address": "%s", "target": "@replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Target:  "@replica",
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			desc:    "Open() (defaults omitted)",
			connStr: fmt.Sprintf(`{"address": "%s", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			desc:    "Open() with keyspace",
			connStr: fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "target": "ks:0@replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Protocol: "grpc",
					Target:   "ks:0@replica",
					Timeout:  30 * time.Second,
				},
			},
		},
	}

	for _, tc := range testcases {
		c, err := drv{}.Open(tc.connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		wantc := tc.conn
		newc := *(c.(*conn))
		newc.Address = ""
		newc.conn = nil
		newc.session = nil
		if !reflect.DeepEqual(&newc, wantc) {
			t.Errorf("%v: conn:\n%+v, want\n%+v", tc.desc, &newc, wantc)
		}
	}
}

func TestOpen_UnregisteredProtocol(t *testing.T) {
	_, err := drv{}.Open(`{"protocol": "none"}`)
	want := "no dialer registered for VTGate protocol none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, want %s", err, want)
	}
}

func TestOpen_InvalidJson(t *testing.T) {
	_, err := drv{}.Open(`{`)
	want := "unexpected end of JSON input"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, want %s", err, want)
	}
}

func TestExec(t *testing.T) {
	config := Configuration{
		Target:  "@rdonly",
		Timeout: 30 * time.Second,
	}

	db, err := Open(testAddress, "@rdonly", config.Timeout)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, err := db.Prepare("request")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := s.Exec(int64(0))
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := r.LastInsertId(); v != 72 {
		t.Errorf("insert id: %d, want 72", v)
	}
	if v, _ := r.RowsAffected(); v != 123 {
		t.Errorf("rows affected: %d, want 123", v)
	}

	s2, err := db.Prepare("none")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	_, err = s2.Exec(nil)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}

func TestConfigurationToJSON(t *testing.T) {
	config := Configuration{
		Protocol:  "some-invalid-protocol",
		Target:    "ks2",
		Streaming: true,
		Timeout:   1 * time.Second,
	}
	want := `{"Protocol":"some-invalid-protocol","Address":"","Target":"ks2","Streaming":true,"Timeout":1000000000}`

	json, err := config.toJSON()
	if err != nil {
		t.Fatal(err)
	}
	if json != want {
		t.Errorf("Configuration.JSON(): got: %v want: %v", json, want)
	}
}

func TestExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "@rdonly", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	s, err := db.Prepare("request")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Exec(int64(0))
	want := "Exec not allowed for streaming connections"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}

func TestQuery(t *testing.T) {
	var testcases = []struct {
		desc        string
		config      Configuration
		requestName string
	}{
		{
			desc: "non-streaming, vtgate",
			config: Configuration{
				Protocol: "grpc",
				Address:  testAddress,
				Target:   "@rdonly",
				Timeout:  30 * time.Second,
			},
			requestName: "request",
		},
		{
			desc: "streaming, vtgate",
			config: Configuration{
				Protocol:  "grpc",
				Address:   testAddress,
				Target:    "@rdonly",
				Timeout:   30 * time.Second,
				Streaming: true,
			},
			requestName: "request",
		},
	}

	for _, tc := range testcases {
		db, err := OpenWithConfiguration(tc.config)
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer db.Close()

		s, err := db.Prepare(tc.requestName)
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer s.Close()

		r, err := s.Query(int64(0))
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer r.Close()
		cols, err := r.Columns()
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		wantCols := []string{
			"field1",
			"field2",
		}
		if !reflect.DeepEqual(cols, wantCols) {
			t.Errorf("%v: cols: %v, want %v", tc.desc, cols, wantCols)
		}
		count := 0
		wantValues := []struct {
			field1 int16
			field2 string
		}{{1, "value1"}, {2, "value2"}}
		for r.Next() {
			var field1 int16
			var field2 string
			err := r.Scan(&field1, &field2)
			if err != nil {
				t.Errorf("%v: %v", tc.desc, err)
			}
			if want := wantValues[count].field1; field1 != want {
				t.Errorf("%v: wrong value for field1: got: %v want: %v", tc.desc, field1, want)
			}
			if want := wantValues[count].field2; field2 != want {
				t.Errorf("%v: wrong value for field2: got: %v want: %v", tc.desc, field2, want)
			}
			count++
		}
		if count != len(wantValues) {
			t.Errorf("%v: count: %d, want %d", tc.desc, count, len(wantValues))
		}

		s2, err := db.Prepare("none")
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer s2.Close()

		rows, err := s2.Query(nil)
		want := "no match for: none"
		if tc.config.Streaming && err == nil {
			defer rows.Close()
			// gRPC requires to consume the stream first before the error becomes visible.
			if rows.Next() {
				t.Errorf("%v: query should not have returned anything but did.", tc.desc)
			}
			err = rows.Err()
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: err: %v, does not contain %s", tc.desc, err, want)
		}
	}
}

func TestTx(t *testing.T) {
	c := Configuration{
		Protocol: "grpc",
		Address:  testAddress,
		Target:   "@master",
		Timeout:  30 * time.Second,
	}

	db, err := OpenWithConfiguration(c)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	s, err := tx.Prepare("txRequest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Exec(int64(0))
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	// Commit on committed transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Commit()
	want := "sql: Transaction has already been committed or rolled back"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}

	// Test rollback now.
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	s, err = tx.Prepare("txRequest")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Query(int64(0))
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	// Rollback on rolled back transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Rollback()
	want = "sql: Transaction has already been committed or rolled back"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}

func TestTxExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "@rdonly", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	want := "Exec not allowed for streaming connection"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}
