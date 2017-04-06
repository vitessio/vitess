// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
			connStr: fmt.Sprintf(`{"address": "%s", "tablet_type": "replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Protocol:   "grpc",
					TabletType: "replica",
					Timeout:    30 * time.Second,
				},
				tabletTypeProto: topodatapb.TabletType_REPLICA,
			},
		},
		{
			desc:    "Open() (defaults omitted)",
			connStr: fmt.Sprintf(`{"address": "%s", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Protocol:   "grpc",
					TabletType: "master",
					Timeout:    30 * time.Second,
				},
				tabletTypeProto: topodatapb.TabletType_MASTER,
			},
		},
		{
			desc:    "Open() with keyspace",
			connStr: fmt.Sprintf(`{"address": "%s", "keyspace": "ks", "shard": "0", "tablet_type": "replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				Configuration: Configuration{
					Protocol:   "grpc",
					Keyspace:   "ks",
					TabletType: "replica",
					Timeout:    30 * time.Second,
				},
				tabletTypeProto: topodatapb.TabletType_REPLICA,
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
		newc.vtgateConn = nil
		if !reflect.DeepEqual(&newc, wantc) {
			t.Errorf("%v: conn: %+v, want %+v", tc.desc, &newc, wantc)
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

func TestOpen_ValidTabletTypeRequired(t *testing.T) {
	_, err := drv{}.Open(`{"tablet_type": "foobar"}`)
	want := "unknown TabletType foobar"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, want %s", err, want)
	}
}

func TestExec(t *testing.T) {
	var testcases = []struct {
		desc        string
		config      Configuration
		requestName string
	}{
		{
			desc: "vtgate",
			config: Configuration{
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
			},
			requestName: "request",
		},
		{
			desc: "vtgate with keyspace",
			config: Configuration{
				Keyspace:   "ks",
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
			},
			requestName: "requestKeyspace",
		},
	}

	for _, tc := range testcases {
		db, err := Open(testAddress, tc.config.Keyspace, tc.config.TabletType, tc.config.Timeout)
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer db.Close()

		s, err := db.Prepare(tc.requestName)
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer s.Close()

		r, err := s.Exec(int64(0))
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		if v, _ := r.LastInsertId(); v != 72 {
			t.Errorf("%v: insert id: %d, want 72", tc.desc, v)
		}
		if v, _ := r.RowsAffected(); v != 123 {
			t.Errorf("%v: rows affected: %d, want 123", tc.desc, v)
		}

		s2, err := db.Prepare("none")
		if err != nil {
			t.Errorf("%v: %v", tc.desc, err)
		}
		defer s2.Close()

		_, err = s2.Exec(nil)
		want := "no match for: none"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v: err: %v, does not contain %s", tc.desc, err, want)
		}
	}
}

func TestConfigurationToJSON(t *testing.T) {
	var testcases = []struct {
		desc   string
		config Configuration
		json   string
	}{
		{
			desc: "all fields set",
			config: Configuration{
				Protocol:   "some-invalid-protocol",
				Keyspace:   "ks2",
				TabletType: "replica",
				Streaming:  true,
				Timeout:    1 * time.Second,
			},
			json: `{"Protocol":"some-invalid-protocol","Address":"","Keyspace":"ks2","tablet_type":"replica","Streaming":true,"Timeout":1000000000}`,
		},
		{
			desc: "default fields are empty",
			config: Configuration{
				Keyspace: "ks2",
				Timeout:  1 * time.Second,
			},
			json: `{"Protocol":"grpc","Address":"","Keyspace":"ks2","tablet_type":"master","Streaming":false,"Timeout":1000000000}`,
		},
	}

	for _, tc := range testcases {
		json, err := tc.config.toJSON()
		if err != nil {
			t.Errorf("%v: JSON conversion should have succeeded but did not: %v", tc.desc, err)
		}
		if json != tc.json {
			t.Errorf("%v: Configuration.JSON(): got: %v want: %v Configuration: %v", tc.desc, json, tc.json, tc.config)
		}
	}
}

func TestExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "", "rdonly", 30*time.Second)
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
				Protocol:   "grpc",
				Address:    testAddress,
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
			},
			requestName: "request",
		},
		{
			desc: "non-streaming, vtgate with keyspace",
			config: Configuration{
				Protocol:   "grpc",
				Address:    testAddress,
				Keyspace:   "ks",
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
			},
			requestName: "requestKeyspace",
		},
		{
			desc: "streaming, vtgate",
			config: Configuration{
				Protocol:   "grpc",
				Address:    testAddress,
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
				Streaming:  true,
			},
			requestName: "request",
		},
		{
			desc: "streaming, vtgate with keyspace",
			config: Configuration{
				Protocol:   "grpc",
				Address:    testAddress,
				Keyspace:   "ks",
				TabletType: "rdonly",
				Timeout:    30 * time.Second,
				Streaming:  true,
			},
			requestName: "requestKeyspace",
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
	var testcases = []struct {
		desc        string
		config      Configuration
		requestName string
	}{
		{
			desc: "vtgate",
			config: Configuration{
				Protocol:   "grpc",
				Address:    testAddress,
				TabletType: "master",
				Timeout:    30 * time.Second,
			},
			requestName: "txRequest",
		},
		{
			desc: "vtgate with keyspace",
			config: Configuration{
				Protocol:   "grpc",
				Address:    testAddress,
				Keyspace:   "ks",
				TabletType: "master",
				Timeout:    30 * time.Second,
			},
			requestName: "txRequestKeyspace",
		},
	}

	for _, tc := range testcases {
		testTxCommit(t, tc.config, tc.desc, tc.requestName)

		testTxRollback(t, tc.config, tc.desc, tc.requestName)
	}
}

func testTxCommit(t *testing.T, c Configuration, desc, requestName string) {
	db, err := OpenWithConfiguration(c)
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}

	s, err := tx.Prepare(requestName)
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}

	_, err = s.Exec(int64(0))
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	err = tx.Commit()
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	// Commit on committed transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Commit()
	want := "sql: Transaction has already been committed or rolled back"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v: err: %v, does not contain %s", desc, err, want)
	}
}

func testTxRollback(t *testing.T, c Configuration, desc, requestName string) {
	db, err := OpenWithConfiguration(c)
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	s, err := tx.Prepare(requestName)
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	_, err = s.Query(int64(0))
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Errorf("%v: %v", desc, err)
	}
	// Rollback on rolled back transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Rollback()
	want := "sql: Transaction has already been committed or rolled back"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%v: err: %v, does not contain %s", desc, err, want)
	}
}

func TestTxExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "", "rdonly", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Begin()
	want := "transaction not allowed for streaming connection"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}
