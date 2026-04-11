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

package vitessdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
)

var testAddress string

// TestMain tests the Vitess Go SQL driver.
//
// Note that the queries used in the test are not valid SQL queries and don't
// have to be. The main point here is to test the interactions against a
// vtgate implementation (here: fakeVTGateService from fakeserver_test.go).
func TestMain(m *testing.M) {
	service := CreateFakeServer()

	// listen on a random port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
	locationPST, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}

	testcases := []struct {
		desc    string
		connStr string
		conn    *conn
	}{
		{
			desc:    "Open()",
			connStr: fmt.Sprintf(`{"address": "%s", "target": "@replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				cfg: Configuration{
					Protocol:   "grpc",
					DriverName: "vitess",
					Target:     "@replica",
				},
				convert: &converter{
					location: time.UTC,
				},
			},
		},
		{
			desc:    "Open() (defaults omitted)",
			connStr: fmt.Sprintf(`{"address": "%s", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				cfg: Configuration{
					Protocol:   "grpc",
					DriverName: "vitess",
				},
				convert: &converter{
					location: time.UTC,
				},
			},
		},
		{
			desc:    "Open() with keyspace",
			connStr: fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "target": "ks:0@replica", "timeout": %d}`, testAddress, int64(30*time.Second)),
			conn: &conn{
				cfg: Configuration{
					Protocol:   "grpc",
					DriverName: "vitess",
					Target:     "ks:0@replica",
				},
				convert: &converter{
					location: time.UTC,
				},
			},
		},
		{
			desc: "Open() with custom timezone",
			connStr: fmt.Sprintf(
				`{"address": "%s", "timeout": %d, "defaultlocation": "America/Los_Angeles"}`,
				testAddress, int64(30*time.Second)),
			conn: &conn{
				cfg: Configuration{
					Protocol:        "grpc",
					DriverName:      "vitess",
					DefaultLocation: "America/Los_Angeles",
				},
				convert: &converter{
					location: locationPST,
				},
			},
		},
	}

	for _, tc := range testcases {
		c, err := drv{}.Open(tc.connStr)
		require.NoError(t, err)
		defer c.Close()

		wantc := tc.conn
		newc := *(c.(*conn))
		newc.cfg.Address = ""
		newc.conn = nil
		newc.session = nil
		require.Equal(t, wantc, &newc, tc.desc)
	}
}

func TestOpen_UnregisteredProtocol(t *testing.T) {
	_, err := drv{}.Open(`{"protocol": "none"}`)
	require.ErrorContains(t, err, "no dialer registered for VTGate protocol none")
}

func TestOpen_InvalidJson(t *testing.T) {
	_, err := drv{}.Open(`{`)
	require.ErrorContains(t, err, "unexpected end of JSON input")
}

func TestBeginIsolation(t *testing.T) {
	db, err := Open(testAddress, "@primary")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.EqualError(t, err, errIsolationUnsupported.Error())
}

func TestExec(t *testing.T) {
	db, err := Open(testAddress, "@rdonly")
	require.NoError(t, err)
	defer db.Close()

	s, err := db.Prepare("request")
	require.NoError(t, err)
	defer s.Close()

	r, err := s.Exec(int64(0))
	require.NoError(t, err)
	v, err := r.LastInsertId()
	require.NoError(t, err)
	require.EqualValues(t, 72, v)
	v, err = r.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 123, v)

	s2, err := db.Prepare("none")
	require.NoError(t, err)
	defer s2.Close()

	_, err = s2.Exec()
	require.ErrorContains(t, err, "no match for: none")
}

func TestConfigurationToJSON(t *testing.T) {
	config := Configuration{
		Protocol:        "some-invalid-protocol",
		Target:          "ks2",
		Streaming:       true,
		DefaultLocation: "Local",
	}
	want := `{"Protocol":"some-invalid-protocol","Address":"","Target":"ks2","Streaming":true,"DefaultLocation":"Local","SessionToken":""}`

	json, err := config.toJSON()
	require.NoError(t, err)
	require.Equal(t, want, json)
}

func TestExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "@rdonly")
	require.NoError(t, err)

	s, err := db.Prepare("request")
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Exec(int64(0))
	require.ErrorContains(t, err, "Exec not allowed for streaming connections")
}

func TestQuery(t *testing.T) {
	testcases := []struct {
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
			},
			requestName: "request",
		},
		{
			desc: "streaming, vtgate",
			config: Configuration{
				Protocol:  "grpc",
				Address:   testAddress,
				Target:    "@rdonly",
				Streaming: true,
			},
			requestName: "request",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			db, err := OpenWithConfiguration(tc.config)
			require.NoError(t, err)
			defer db.Close()

			s, err := db.Prepare(tc.requestName)
			require.NoError(t, err)
			defer s.Close()

			r, err := s.Query(int64(0))
			require.NoError(t, err)
			defer r.Close()
			cols, err := r.Columns()
			require.NoError(t, err)
			wantCols := []string{
				"field1",
				"field2",
			}
			require.Equal(t, wantCols, cols)
			count := 0
			wantValues := []struct {
				field1 int16
				field2 string
			}{{1, "value1"}, {2, "value2"}}
			for r.Next() {
				var field1 int16
				var field2 string
				err := r.Scan(&field1, &field2)
				require.NoError(t, err)
				require.Equal(t, wantValues[count].field1, field1)
				require.Equal(t, wantValues[count].field2, field2)
				count++
			}
			require.Equal(t, len(wantValues), count)

			s2, err := db.Prepare("none")
			require.NoError(t, err)
			defer s2.Close()

			rows, err := s2.Query()
			if tc.config.Streaming && err == nil {
				defer rows.Close()
				// gRPC requires to consume the stream first before the error becomes visible.
				require.False(t, rows.Next())
				err = rows.Err()
			}
			require.ErrorContains(t, err, "no match for: none")
		})
	}
}

func TestBindVars(t *testing.T) {
	testcases := []struct {
		desc   string
		in     []driver.NamedValue
		out    map[string]*querypb.BindVariable
		outErr string
	}{{
		desc: "all names",
		in: []driver.NamedValue{{
			Name:  "n1",
			Value: int64(0),
		}, {
			Name:  "n2",
			Value: "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"n1": sqltypes.Int64BindVariable(0),
			"n2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "prefixed names",
		in: []driver.NamedValue{{
			Name:  ":n1",
			Value: int64(0),
		}, {
			Name:  "@n2",
			Value: "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"n1": sqltypes.Int64BindVariable(0),
			"n2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "all positional",
		in: []driver.NamedValue{{
			Ordinal: 1,
			Value:   int64(0),
		}, {
			Ordinal: 2,
			Value:   "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"v1": sqltypes.Int64BindVariable(0),
			"v2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "name, then position",
		in: []driver.NamedValue{{
			Name:  "n1",
			Value: int64(0),
		}, {
			Ordinal: 2,
			Value:   "abcd",
		}},
		outErr: errNoIntermixing.Error(),
	}, {
		desc: "position, then name",
		in: []driver.NamedValue{{
			Ordinal: 1,
			Value:   int64(0),
		}, {
			Name:  "n2",
			Value: "abcd",
		}},
		outErr: errNoIntermixing.Error(),
	}}

	converter := &converter{}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			bv, err := converter.bindVarsFromNamedValues(tc.in)
			if tc.outErr != "" {
				require.EqualError(t, err, tc.outErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.out, bv)
			}
		})
	}
}

func TestDatetimeQuery(t *testing.T) {
	testcases := []struct {
		desc        string
		config      Configuration
		requestName string
	}{
		{
			desc: "datetime & date, vtgate",
			config: Configuration{
				Protocol: "grpc",
				Address:  testAddress,
				Target:   "@rdonly",
			},
			requestName: "requestDates",
		},
		{
			desc: "datetime & date (local timezone), vtgate",
			config: Configuration{
				Protocol:        "grpc",
				Address:         testAddress,
				Target:          "@rdonly",
				DefaultLocation: "Local",
			},
			requestName: "requestDates",
		},
		{
			desc: "datetime & date, streaming, vtgate",
			config: Configuration{
				Protocol:  "grpc",
				Address:   testAddress,
				Target:    "@rdonly",
				Streaming: true,
			},
			requestName: "requestDates",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			db, err := OpenWithConfiguration(tc.config)
			require.NoError(t, err)
			defer db.Close()

			s, err := db.Prepare(tc.requestName)
			require.NoError(t, err)
			defer s.Close()

			r, err := s.Query(0)
			require.NoError(t, err)
			defer r.Close()

			cols, err := r.Columns()
			require.NoError(t, err)
			wantCols := []string{
				"fieldDatetime",
				"fieldDate",
			}
			require.Equal(t, wantCols, cols)

			location := time.UTC
			if tc.config.DefaultLocation != "" {
				location, err = time.LoadLocation(tc.config.DefaultLocation)
				require.NoError(t, err)
			}

			count := 0
			wantValues := []struct {
				fieldDatetime time.Time
				fieldDate     time.Time
			}{{
				time.Date(2009, 3, 29, 17, 22, 11, 0, location),
				time.Date(2006, 7, 2, 0, 0, 0, 0, location),
			}, {
				time.Time{},
				time.Time{},
			}}

			for r.Next() {
				var fieldDatetime time.Time
				var fieldDate time.Time
				err := r.Scan(&fieldDatetime, &fieldDate)
				require.NoError(t, err)
				require.Equal(t, wantValues[count].fieldDatetime, fieldDatetime)
				require.Equal(t, wantValues[count].fieldDate, fieldDate)
				count++
			}

			require.Equal(t, len(wantValues), count)
		})
	}
}

func TestTx(t *testing.T) {
	c := Configuration{
		Protocol: "grpc",
		Address:  testAddress,
		Target:   "@primary",
	}

	db, err := OpenWithConfiguration(c)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	s, err := tx.Prepare("txRequest")
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Exec(int64(0))
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	// Commit on committed transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Commit()
	require.ErrorIs(t, err, sql.ErrTxDone)

	// Test rollback now.
	tx, err = db.Begin()
	require.NoError(t, err)
	s, err = tx.Prepare("txRequest")
	require.NoError(t, err)
	defer s.Close()
	r, err := s.Query(int64(0))
	require.NoError(t, err)
	defer r.Close()
	err = tx.Rollback()
	require.NoError(t, err)
	// Rollback on rolled back transaction is caught by Golang sql package.
	// We actually don't have to cover this in our code.
	err = tx.Rollback()
	require.ErrorIs(t, err, sql.ErrTxDone)
}

func TestTxExecStreamingNotAllowed(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "@rdonly")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Begin()
	require.ErrorContains(t, err, "Exec not allowed for streaming connection")
}

func TestSessionToken(t *testing.T) {
	c := Configuration{
		Protocol: "grpc",
		Address:  testAddress,
		Target:   "@primary",
	}

	ctx := context.Background()

	db, err := OpenWithConfiguration(c)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.Begin()
	require.NoError(t, err)

	s, err := tx.Prepare("txRequest")
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Exec(int64(0))
	require.NoError(t, err)

	sessionToken, err := SessionTokenFromTx(ctx, tx)
	require.NoError(t, err)

	distributedTxConfig := Configuration{
		Address:      testAddress,
		Target:       "@primary",
		SessionToken: sessionToken,
	}

	sameTx, sameValidationFunc, err := DistributedTxFromSessionToken(ctx, distributedTxConfig)
	require.NoError(t, err)

	newS, err := sameTx.Prepare("distributedTxRequest")
	require.NoError(t, err)
	defer newS.Close()

	_, err = newS.Exec(int64(1))
	require.NoError(t, err)

	err = sameValidationFunc()
	require.NoError(t, err)

	// enforce that Rollback can't be called on the distributed tx
	noRollbackTx, noRollbackValidationFunc, err := DistributedTxFromSessionToken(ctx, distributedTxConfig)
	require.NoError(t, err)

	err = noRollbackValidationFunc()
	require.NoError(t, err)

	err = noRollbackTx.Rollback()
	require.EqualError(t, err, "calling Rollback from a distributed tx is not allowed")

	// enforce that Commit can't be called on the distributed tx
	noCommitTx, noCommitValidationFunc, err := DistributedTxFromSessionToken(ctx, distributedTxConfig)
	require.NoError(t, err)

	err = noCommitValidationFunc()
	require.NoError(t, err)

	err = noCommitTx.Commit()
	require.EqualError(t, err, "calling Commit from a distributed tx is not allowed")

	// finally commit the original tx
	err = tx.Commit()
	require.NoError(t, err)
}

// TestStreamExec tests that different kinds of query present in `execMap` can run through streaming api
func TestStreamExec(t *testing.T) {
	db, err := OpenForStreaming(testAddress, "@rdonly")
	require.NoError(t, err)
	defer db.Close()

	for k, v := range createExecMap() {
		t.Run(k, func(t *testing.T) {
			s, err := db.Prepare(k)
			require.NoError(t, err)
			defer s.Close()

			r, err := s.Query(0)
			require.NoError(t, err)
			defer r.Close()

			fields, err := r.Columns()
			require.NoError(t, err)
			require.Equal(t, colList(v.result.Fields), fields)

			for r.Next() {
				require.NoError(t, r.Err())
			}
		})
	}
}

func colList(fields []*querypb.Field) []string {
	if fields == nil {
		return nil
	}
	cols := make([]string, 0, len(fields))
	for _, field := range fields {
		cols = append(cols, field.Name)
	}
	return cols
}

func TestConnSeparateSessions(t *testing.T) {
	c := Configuration{
		Protocol: "grpc",
		Address:  testAddress,
		Target:   "@primary",
	}

	db, err := OpenWithConfiguration(c)
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Each new connection starts a fresh session pointed at @primary. When the
	// USE statement is executed, we simulate a change to that individual
	// connection's target string.
	//
	// No connections are returned to the pool during this test and therefore
	// the connection state should not be shared.
	var conns []*sql.Conn
	for range 3 {
		sconn, err := db.Conn(ctx)
		require.NoError(t, err)
		conns = append(conns, sconn)

		targets := []string{targetString(t, sconn)}

		_, err = sconn.ExecContext(ctx, "use @rdonly")
		require.NoError(t, err)

		targets = append(targets, targetString(t, sconn))

		require.Equal(t, []string{"@primary", "@rdonly"}, targets)
	}

	for _, c := range conns {
		require.NoError(t, c.Close())
	}
}

func TestConnReuseSessions(t *testing.T) {
	c := Configuration{
		Protocol: "grpc",
		Address:  testAddress,
		Target:   "@primary",
	}

	db, err := OpenWithConfiguration(c)
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pull an individual connection from the pool and execute a USE, resulting
	// in changing the target string. We return the connection to the pool
	// continuously in this test and verify that we keep pulling the same
	// connection with its target string altered.
	sconn, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = sconn.ExecContext(ctx, "use @rdonly")
	require.NoError(t, err)
	require.NoError(t, sconn.Close())

	var targets []string
	for range 3 {
		sconn, err := db.Conn(ctx)
		require.NoError(t, err)

		targets = append(targets, targetString(t, sconn))
		require.NoError(t, sconn.Close())
	}

	require.Equal(t, []string{"@rdonly", "@rdonly", "@rdonly"}, targets)
}

func targetString(t *testing.T, c *sql.Conn) string {
	t.Helper()

	var target string
	require.NoError(t, c.Raw(func(driverConn any) error {
		target = driverConn.(*conn).session.SessionPb().TargetString
		return nil
	}))

	return target
}
