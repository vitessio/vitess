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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/vttls"
)

func TestMysql8SetReplicationSourceCommand(t *testing.T) {
	params := &ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
	want := `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor8{}}
	got := conn.SetReplicationSourceCommand(params, host, port, 0, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)

	heartbeatInterval := 5.4
	want = `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_HEARTBEAT_PERIOD = 5.4,
  SOURCE_AUTO_POSITION = 1`

	got = conn.SetReplicationSourceCommand(params, host, port, heartbeatInterval, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor.SetReplicationSourceCommand(%#v, %#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, heartbeatInterval, connectRetry, got, want)
}

func TestMysql8SetReplicationSourceCommandSSL(t *testing.T) {
	params := &ConnParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
	want := `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  SOURCE_SSL = 1,
  SOURCE_SSL_CA = 'ssl-ca',
  SOURCE_SSL_CAPATH = 'ssl-ca-path',
  SOURCE_SSL_CERT = 'ssl-cert',
  SOURCE_SSL_KEY = 'ssl-key',
  SOURCE_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor8{}}
	got := conn.SetReplicationSourceCommand(params, host, port, 0, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)
}

// TestMysql8SetReplicationSourceCommandPreferredSSLWithUnixSocket verifies that
// a Unix socket for direct connections does not disable TLS for TCP replication.
func TestMysql8SetReplicationSourceCommandPreferredSSLWithUnixSocket(t *testing.T) {
	params := &ConnParams{
		Uname:      "username",
		Pass:       "password",
		UnixSocket: "/var/run/mysqld/mysqld.sock",
		SslMode:    vttls.Preferred,
	}
	want := `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'primary.example.com',
  SOURCE_PORT = 3306,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 10,
  SOURCE_SSL = 1,
  SOURCE_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor8{}}
	got := conn.SetReplicationSourceCommand(params, "primary.example.com", 3306, 0, 10)
	assert.Equal(t, want, got)
}

// TestMysqlLegacySetReplicationSourceCommandPreferredSSLWithUnixSocket verifies
// the same behavior for MySQL versions that use legacy replication terminology.
func TestMysqlLegacySetReplicationSourceCommandPreferredSSLWithUnixSocket(t *testing.T) {
	params := &ConnParams{
		Uname:      "username",
		Pass:       "password",
		UnixSocket: "/var/run/mysqld/mysqld.sock",
		SslMode:    vttls.Preferred,
	}
	want := `CHANGE MASTER TO
  MASTER_HOST = 'primary.example.com',
  MASTER_PORT = 3306,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 10,
  MASTER_SSL = 1,
  MASTER_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor57{}}
	got := conn.SetReplicationSourceCommand(params, "primary.example.com", 3306, 0, 10)
	assert.Equal(t, want, got)
}

func TestMysql8SetReplicationPositionCommands(t *testing.T) {
	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	conn := &Conn{flavor: mysqlFlavor8{}}
	queries := conn.SetReplicationPositionCommands(pos)
	assert.Equal(t, []string{"RESET MASTER", "SET GLOBAL gtid_purged = ''"}, queries)
}

func TestMysql82SetReplicationPositionCommands(t *testing.T) {
	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	conn := &Conn{flavor: mysqlFlavor82{}}
	queries := conn.SetReplicationPositionCommands(pos)
	assert.Equal(t, []string{"RESET BINARY LOGS AND GTIDS", "SET GLOBAL gtid_purged = ''"}, queries)
}

func TestMysql8ResetReplicationParametersCommands(t *testing.T) {
	conn := &Conn{flavor: mysqlFlavor8{}}
	queries := conn.ResetReplicationParametersCommands()
	assert.Equal(t, []string{"RESET REPLICA ALL"}, queries)
}

func TestMysql82ResetReplicationParametersCommands(t *testing.T) {
	conn := &Conn{flavor: mysqlFlavor82{}}
	queries := conn.ResetReplicationParametersCommands()
	assert.Equal(t, []string{"RESET REPLICA ALL"}, queries)
}

func TestMysql9SetReplicationPositionCommands(t *testing.T) {
	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	conn := &Conn{flavor: mysqlFlavor9{}}
	queries := conn.SetReplicationPositionCommands(pos)
	assert.Equal(t, []string{"RESET BINARY LOGS AND GTIDS", "SET GLOBAL gtid_purged = ''"}, queries)
}

func TestMysql9ResetReplicationParametersCommands(t *testing.T) {
	conn := &Conn{flavor: mysqlFlavor9{}}
	queries := conn.ResetReplicationParametersCommands()
	assert.Equal(t, []string{"RESET REPLICA ALL"}, queries)
}

func TestMysql9SetReplicationSourceCommand(t *testing.T) {
	params := &ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
	want := `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor9{}}
	got := conn.SetReplicationSourceCommand(params, host, port, 0, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor9.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)

	heartbeatInterval := 5.4
	want = `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_HEARTBEAT_PERIOD = 5.4,
  SOURCE_AUTO_POSITION = 1`

	got = conn.SetReplicationSourceCommand(params, host, port, heartbeatInterval, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor9.SetReplicationSourceCommand(%#v, %#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, heartbeatInterval, connectRetry, got, want)
}

func TestMysql9SetReplicationSourceCommandSSL(t *testing.T) {
	params := &ConnParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
	want := `CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'localhost',
  SOURCE_PORT = 123,
  SOURCE_USER = 'username',
  SOURCE_PASSWORD = 'password',
  SOURCE_CONNECT_RETRY = 1234,
  SOURCE_SSL = 1,
  SOURCE_SSL_CA = 'ssl-ca',
  SOURCE_SSL_CAPATH = 'ssl-ca-path',
  SOURCE_SSL_CERT = 'ssl-cert',
  SOURCE_SSL_KEY = 'ssl-key',
  SOURCE_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor9{}}
	got := conn.SetReplicationSourceCommand(params, host, port, 0, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor9.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)
}
