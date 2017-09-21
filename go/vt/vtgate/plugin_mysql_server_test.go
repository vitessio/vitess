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
	"golang.org/x/net/context"
	"os"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
)

type testHandler struct {
	lastConn *mysql.Conn
}

func (th *testHandler) NewConnection(c *mysql.Conn) {
	th.lastConn = c
}

func (th *testHandler) ConnectionClosed(c *mysql.Conn) {
}

func (th *testHandler) ComQuery(c *mysql.Conn, q []byte, callback func(*sqltypes.Result) error) error {
	return nil
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

	unixSocket := "/tmp/mysql_vitess_test.sock"

	l, err := newMysqlUnixSocket(unixSocket, authServer, th)
	if err != nil {
		t.Fatalf("NewUnixSocket failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket,
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

	unixSocket := "/tmp/mysql_vitess_test.sock"

	_, err := os.Create(unixSocket)
	if err != nil {
		t.Fatalf("NewListener failed to create file: %v", err)
	}

	l, err := newMysqlUnixSocket(unixSocket, authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()

	params := &mysql.ConnParams{
		UnixSocket: unixSocket,
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

	unixSocket := "/tmp/mysql_vitess_test.sock"

	l, err := newMysqlUnixSocket(unixSocket, authServer, th)
	if err != nil {
		t.Errorf("NewListener failed: %v", err)
	}
	defer l.Close()
	go func() {
		l.Accept()
	}()
	_, err = newMysqlUnixSocket(unixSocket, authServer, th)
	want := "listen unix /tmp/mysql_vitess_test.sock"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Error: %v, want prefix %s", err, want)
	}
}
