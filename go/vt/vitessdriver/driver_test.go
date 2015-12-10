// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vitessdriver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
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

func TestDriver(t *testing.T) {
	connStr := fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "timeout": %d}`, testAddress, int64(30*time.Second))
	db, err := sql.Open("vitess", connStr)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Query("request1", int64(0))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for r.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("count: %d, want 2", count)
	}
	_ = db.Close()
}

func TestOpen(t *testing.T) {
	connStr := fmt.Sprintf(`{"address": "%s", "tablet_type": "replica", "timeout": %d}`, testAddress, int64(30*time.Second))
	c, err := drv{}.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wantc := &conn{
		Configuration: Configuration{
			Protocol:   "grpc",
			TabletType: "replica",
			Streaming:  false,
			Timeout:    30 * time.Second,
		},
		tabletTypeProto: topodatapb.TabletType_REPLICA,
	}
	newc := *(c.(*conn))
	newc.Address = ""
	newc.vtgateConn = nil
	if !reflect.DeepEqual(&newc, wantc) {
		t.Errorf("conn: %+v, want %+v", &newc, wantc)
	}
}

func TestOpenShard(t *testing.T) {
	connStr := fmt.Sprintf(`{"address": "%s", "keyspace": "ks1", "shard": "0", "tablet_type": "replica", "timeout": %d}`, testAddress, int64(30*time.Second))
	c, err := drv{}.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wantc := &conn{
		Configuration: Configuration{
			Protocol:   "grpc",
			Keyspace:   "ks1",
			Shard:      "0",
			TabletType: "replica",
			Streaming:  false,
			Timeout:    30 * time.Second,
		},
		tabletTypeProto: topodatapb.TabletType_REPLICA,
	}
	newc := *(c.(*conn))
	newc.Address = ""
	newc.vtgateConn = nil
	if !reflect.DeepEqual(&newc, wantc) {
		t.Errorf("conn: %+v, want %+v", &newc, wantc)
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

func TestOpen_KeyspaceAndShardBelongTogether(t *testing.T) {
	_, err := drv{}.Open(`{"keyspace": "ks1"}`)
	want := "Always set both keyspace and shard or leave both empty."
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, want %s", err, want)
	}

	_, err = drv{}.Open(`{"shard": "0"}`)
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
		dataSourceName string
		requestName    string
	}{
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1",
		},
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "keyspace": "ks1", "shard": "0", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1SpecificShard",
		},
	}

	for _, tc := range testcases {
		connStr := fmt.Sprintf(tc.dataSourceName, testAddress, int64(30*time.Second))
		c, err := drv{}.Open(connStr)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := c.Prepare(tc.requestName)
		if ni := s.NumInput(); ni != -1 {
			t.Errorf("got %d, want -1", ni)
		}
		r, err := s.Exec([]driver.Value{int64(0)})
		if err != nil {
			t.Error(err)
		}
		if v, _ := r.LastInsertId(); v != 72 {
			t.Errorf("insert id: %d, want 72", v)
		}
		if v, _ := r.RowsAffected(); v != 123 {
			t.Errorf("rows affected: %d, want 123", v)
		}
		_ = s.Close()

		s, _ = c.Prepare("none")
		_, err = s.Exec(nil)
		want := "no match for: none"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err: %v, does not contain %s", err, want)
		}
		_ = c.Close()
	}
}

func TestExecStreamingNotAllowed(t *testing.T) {
	connStr := fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "streaming": true, "timeout": %d}`, testAddress, int64(30*time.Second))
	c, err := drv{}.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := c.Prepare("request1")
	_, err = s.Exec([]driver.Value{int64(0)})
	want := "Exec not allowed for streaming connections"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
}

func TestQuery(t *testing.T) {
	var testcases = []struct {
		dataSourceName string
		requestName    string
	}{
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1",
		},
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "keyspace": "ks1", "shard": "0", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1SpecificShard",
		},
	}

	for _, tc := range testcases {
		connStr := fmt.Sprintf(tc.dataSourceName, testAddress, int64(30*time.Second))
		c, err := drv{}.Open(connStr)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := c.Prepare(tc.requestName)
		r, err := s.Query([]driver.Value{int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		cols := r.Columns()
		wantCols := []string{
			"field1",
			"field2",
		}
		if !reflect.DeepEqual(cols, wantCols) {
			t.Fatalf("cols: %v, want %v", cols, wantCols)
		}
		row := make([]driver.Value, 2)
		count := 0
		for {
			err = r.Next(row)
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Error(err)
			}
			count++
		}
		if count != 2 {
			t.Errorf("count: %d, want 2", count)
		}
		_ = s.Close()

		s, _ = c.Prepare("none")
		_, err = s.Query(nil)
		want := "no match for: none"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err: %v, does not contain %s", err, want)
		}
		_ = c.Close()
	}
}

func TestQueryStreaming(t *testing.T) {
	var testcases = []struct {
		dataSourceName string
		requestName    string
	}{
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1",
		},
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "keyspace": "ks1", "shard": "0", "tablet_type": "rdonly", "timeout": %d}`,
			requestName:    "request1SpecificShard",
		},
	}

	for _, tc := range testcases {
		connStr := fmt.Sprintf(tc.dataSourceName, testAddress, int64(30*time.Second))
		c, err := drv{}.Open(connStr)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := c.Prepare(tc.requestName)
		r, err := s.Query([]driver.Value{int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		cols := r.Columns()
		wantCols := []string{
			"field1",
			"field2",
		}
		if !reflect.DeepEqual(cols, wantCols) {
			t.Fatalf("cols: %v, want %v", cols, wantCols)
		}
		row := make([]driver.Value, 2)
		count := 0
		for {
			err = r.Next(row)
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Fatal(err)
			}
			count++
		}
		if count != 2 {
			t.Errorf("count: %d, want 2", count)
		}
		_ = s.Close()
		_ = c.Close()
	}
}

func TestTx(t *testing.T) {
	var testcases = []struct {
		dataSourceName string
		requestName    string
	}{
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "tablet_type": "master", "timeout": %d}`,
			requestName:    "txRequest",
		},
		{
			dataSourceName: `{"protocol": "grpc", "address": "%s", "keyspace": "ks1", "shard": "0", "tablet_type": "master", "timeout": %d}`,
			requestName:    "txRequestSpecificShard",
		},
	}

	for _, tc := range testcases {
		connStr := fmt.Sprintf(tc.dataSourceName, testAddress, int64(30*time.Second))
		c, err := drv{}.Open(connStr)
		if err != nil {
			t.Fatalf("%v: %v", tc.requestName, err)
		}
		tx, err := c.Begin()
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		s, _ := c.Prepare(tc.requestName)
		_, err = s.Exec([]driver.Value{int64(0)})
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		err = tx.Commit()
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		err = tx.Commit()
		want := "commit: not in transaction"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("case: %v err: %v, does not contain %s", tc.requestName, err, want)
		}
		_ = c.Close()

		c, err = drv{}.Open(connStr)
		if err != nil {
			t.Fatalf("%v: %v", tc.requestName, err)
		}
		tx, err = c.Begin()
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		s, _ = c.Prepare(tc.requestName)
		_, err = s.Query([]driver.Value{int64(0)})
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		err = tx.Rollback()
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		err = tx.Rollback()
		if err != nil {
			t.Errorf("%v: %v", tc.requestName, err)
		}
		_ = c.Close()
	}
}

func TestTxExecStreamingNotAllowed(t *testing.T) {
	connStr := fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "tablet_type": "rdonly", "streaming": true, "timeout": %d}`, testAddress, int64(30*time.Second))
	c, err := drv{}.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Begin()
	want := "transaction not allowed for streaming connection"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err: %v, does not contain %s", err, want)
	}
	_ = c.Close()
}
