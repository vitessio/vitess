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

package vtgate

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/trace"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type testHandler struct {
	lastConn *mysql.Conn
}

func (th *testHandler) NewConnection(c *mysql.Conn) {
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(c *mysql.Conn) {
}

func (th *testHandler) ComQuery(c *mysql.Conn, q string, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) ComPrepare(c *mysql.Conn, q string) ([]*querypb.Field, error) {
	return nil, nil
}

func (th *testHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := mysql.NewAuthServerStatic()

	authServer.Entries["user1"] = []*mysql.AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	// Use tmp file to reserve a path, remove it immediately, we only care about
	// name in this context
	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Fatalf("NewUnixSocket failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionStaleUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := mysql.NewAuthServerStatic()

	authServer.Entries["user1"] = []*mysql.AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	// First let's create a file. In this way, we simulate
	// having a stale socket on disk that needs to be cleaned up.
	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := mysql.Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionRespectsExistingUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := mysql.NewAuthServerStatic()

	authServer.Entries["user1"] = []*mysql.AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	if err != nil {
		t.Errorf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()
	_, err = newMysqlUnixSocket(unixSocket.Name(), authServer, th)
	want := "listen unix"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
}

var newSpanOK = func(ctx context.Context, label string) (trace.Span, context.Context) {
	return trace.NoopSpan{}, context.Background()
}

var newFromStringOK = func(ctx context.Context, spanContext, label string) (trace.Span, context.Context, error) {
	return trace.NoopSpan{}, context.Background(), nil
}

func newFromStringFail(t *testing.T) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		t.Fatalf("we didn't provide a parent span in the sql query. this should not have been called. got: %v", parentSpan)
		return trace.NoopSpan{}, context.Background(), nil
	}
}

func newFromStringExpect(t *testing.T, expected string) func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
	return func(ctx context.Context, parentSpan string, label string) (trace.Span, context.Context, error) {
		assert.Equal(t, expected, parentSpan)
		return trace.NoopSpan{}, context.Background(), nil
	}
}

func newSpanFail(t *testing.T) func(ctx context.Context, label string) (trace.Span, context.Context) {
	return func(ctx context.Context, label string) (trace.Span, context.Context) {
		t.Fatalf("we provided a span context but newFromString was not used as expected")
		return trace.NoopSpan{}, context.Background()
	}
}

func TestNoSpanContextPassed(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "sql without comments", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextNoPassedInButExistsInString(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "SELECT * FROM SOMETABLE WHERE COL = \"/*VT_SPAN_CONTEXT=123*/", "someLabel", newSpanOK, newFromStringFail(t))
	assert.NoError(t, err)
}

func TestSpanContextPassedIn(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "/*VT_SPAN_CONTEXT=123*/SQL QUERY", "someLabel", newSpanFail(t), newFromStringOK)
	assert.NoError(t, err)
}

func TestSpanContextPassedInEvenAroundOtherComments(t *testing.T) {
	_, _, err := startSpanTestable(context.Background(), "/*VT_SPAN_CONTEXT=123*/SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ col1, col2 FROM TABLE ", "someLabel",
		newSpanFail(t),
		newFromStringExpect(t, "123"))
	assert.NoError(t, err)
}
