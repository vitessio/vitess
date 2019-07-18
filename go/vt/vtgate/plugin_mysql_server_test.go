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

package vtgate

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/staticauthserver"
	"vitess.io/vitess/go/sqltypes"
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

func (th *testHandler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := staticauthserver.NewAuthServerStatic()

	authServer.Entries["user1"] = []*staticauthserver.AuthServerStaticEntry{
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

	authServer := staticauthserver.NewAuthServerStatic()

	authServer.Entries["user1"] = []*staticauthserver.AuthServerStaticEntry{
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

	authServer := staticauthserver.NewAuthServerStatic()

	authServer.Entries["user1"] = []*staticauthserver.AuthServerStaticEntry{
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
